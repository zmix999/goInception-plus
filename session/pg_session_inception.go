package session

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"gitee.com/zhoujin826/goInception-plus/parser/ast"
	"github.com/jinzhu/gorm"
	"github.com/lib/pq"
	pgDriver "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

func (s *session) PostgreSQLServerVersion() {
	log.Debug("PostgreSQLServerVersion")

	if s.dbVersion > 0 {
		return
	}

	sql := "SELECT current_setting('server_version');"

	rows, err := s.PostgreSQLraw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err == nil {
		s.dbType = DBPostgreSQL
	}
}

func (s *session) PostgreSQLgetTableFromCache(db string, tableName string, indexName string, reportNotExists bool) *TableInfo {
	if db == "" {
		db = s.serach
	}

	if db == "" {
		s.appendErrorNo(ER_WRONG_DB_NAME, "")
		return nil
	}

	key := fmt.Sprintf("%s.%s", db, tableName)
	if s.IgnoreCase() {
		key = strings.ToLower(key)
	}

	if t, ok := s.tableCacheList[key]; ok {
		// 如果表已删除, 之后又使用到,则报错
		if t.IsDeleted {
			if reportNotExists {
				s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, fmt.Sprintf("%s.%s", t.Schema, t.Name))
			}
			return nil
		}
		t.AsName = ""
		return t
	}

	rows := s.PostgreSQLqueryTableFromDB(db, tableName, reportNotExists)
	if rows != nil {
		newT := &TableInfo{
			Schema: db,
			Name:   tableName,
			Fields: rows,
		}
		if rows := s.PostgreSQLqueryIndexFromDB(db, indexName, reportNotExists); rows != nil {
			newT.Indexes = rows
		}
		s.tableCacheList[key] = newT
		return newT
	}
	index := s.PostgreSQLqueryIndexFromDB(db, indexName, reportNotExists)
	if index != nil {
		newT := &TableInfo{
			Schema: db,
			Name:   tableName,
			Fields: rows,
		}
		newT.Indexes = index
		newT.Name = index[0].Table
		s.tableCacheList[key] = newT
		return newT
	}
	return nil
}

func (s *session) PostgreSQLqueryTableFromDB(db string, tableName string, reportNotExists bool) []FieldInfo {
	if db == "" {
		db = s.serach
	}
	var rows []FieldInfo
	sql := fmt.Sprintf("SELECT column_name as \"Field\",concat(data_type,case when character_maximum_length is not null then '(' || character_maximum_length || ')' else NULL end) as \"Type\",collation_name as \"Collation\",is_nullable as \"Null\",column_default as \"Default\" FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s';", db, tableName)

	s.rawScan(sql, &rows)

	if len(rows) == 0 {
		if reportNotExists {
			s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, fmt.Sprintf("%s.%s", db, tableName))
		}
		return nil
	}
	for _, r := range rows {
		r.Table = tableName
	}
	return rows
}

func (s *session) PostgreSQLqueryIndexFromDB(db string, tableName string, reportNotExists bool) []*IndexInfo {
	if db == "" {
		db = s.serach
	}
	var rows []*IndexInfo
	sql := fmt.Sprintf("select tablename as \"Table\" ,indexname as \"Key_name\" from pg_indexes where schemaname='%s' and indexname='%s'", db, tableName)

	s.rawScan(sql, &rows)

	if len(rows) == 0 {
		return nil
	}
	return rows
}

func (s *session) PostgreSQLCheckDBExists(db string, reportNotExists bool) bool {

	if db == "" {
		db = s.dbName
	}

	if db == "" {
		s.appendErrorNo(ER_WRONG_DB_NAME, "")
		return false
	}

	key := db
	if s.IgnoreCase() {
		key = strings.ToLower(db)
	}
	if v, ok := s.dbCacheList[key]; ok {
		return !v.IsDeleted
	}

	sql := "select datname from pg_database where datname = '%s';"

	var name string

	rows, err := s.PostgreSQLraw(fmt.Sprintf(sql, db))
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*pq.Error); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err.Error())
		}
	} else {
		for rows.Next() {
			rows.Scan(&name)
		}
	}

	if name == "" {
		if reportNotExists {
			s.appendErrorNo(ER_DB_NOT_EXISTED_ERROR, db)
		}
		return false
	}

	s.dbCacheList[key] = &DBInfo{
		Name:      db,
		IsNew:     false,
		IsDeleted: false,
	}

	return true
}

func (s *session) PostgreSQLexecuteRemoteStatement(record *Record, isTran bool, isDml bool) {
	log.Debug("PostgreSQLexecuteRemoteStatement")

	sqlStmt := record.Sql + " RETURNING xmin,xmax"

	start := time.Now()

	var res sql.Result
	var err error
	var affectedRows int64

	if isTran {
		res, err = s.PostgreSQLexecDDL(sqlStmt, false)
	}
	if isDml {
		affectedRows = s.PostgreSQLrawScan(sqlStmt)
	} else {
		res, err = s.PostgreSQLexecSQL(sqlStmt, false)
	}

	record.ExecTime = fmt.Sprintf("%.3f", time.Since(start).Seconds())
	record.ExecTimestamp = time.Now().Unix()

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*pgDriver.Error); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err.Error())
		}
		record.StageStatus = StatusExecFail

		// 无法确认是否执行成功,需要通过备份来确认
		if err == nil {
			// 如果没有开启备份,则直接返回
			if s.opt.Backup {
				// 如果是DML语句,则通过备份来验证是否执行成功
				// 如果是DDL语句,则直接报错,由人工确认执行结果,但仍会备份
				switch record.Type.(type) {
				case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
					record.AffectedRows = 0
				default:
					s.appendErrorMsg("The execution result is unknown! Please confirm manually.")
				}

				record.ThreadId = s.PostgreSQLfetchThreadID()
				record.TxID = s.txID
				record.ExecComplete = true
			} else {
				s.appendErrorMsg("The execution result is unknown! Please confirm manually.")
			}
		}

		// log.Infof("[%s] [%d] %s,RowsAffected: %d", s.DBName, s.fetchThreadID(), record.Sql, record.AffectedRows)

		return
	}

	if !isDml {
		affectedRows, err = res.RowsAffected()
		if err != nil {
			s.appendErrorMsg(err.Error())
		}
	}

	record.AffectedRows = affectedRows
	record.TxID = s.txID
	record.ThreadId = s.PostgreSQLfetchThreadID()
	if record.ThreadId == 0 {
		s.appendErrorMsg("无法获取线程号")
	} else {
		record.ExecComplete = true
	}

	record.StageStatus = StatusExecOK

	// log.Infof("[%s] [%d] %s,RowsAffected: %d", s.DBName, s.fetchThreadID(), record.Sql, record.AffectedRows)

	switch node := record.Type.(type) {
	// switch record.Type.(type) {
	case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
		s.totalChangeRows += record.AffectedRows
	case *ast.UseStmt:
		s.dbName = node.DBName
	}

	if _, ok := record.Type.(*ast.CreateTableStmt); ok &&
		record.TableInfo == nil && record.DBName != "" && record.TableName != "" {
		record.TableInfo = s.PostgreSQLgetTableFromCache(record.DBName, record.TableName, "", true)
	} else if _, ok := record.Type.(*ast.CreateSequenceStmt); ok &&
		record.SequencesInfo == nil && record.DBName != "" && record.TableName != "" {
		record.SequencesInfo = s.getSequencesFromCache(record.DBName, record.TableName, true)
	}
}

func (s *session) PostgreSQLfetchThreadID() uint32 {

	if s.threadID > 0 {
		return s.threadID
	}

	var threadId uint64
	sql := "SELECT pg_backend_pid();"
	if s.isMiddleware() {
		sql = s.opt.middlewareExtend + sql
	}

	rows, err := s.PostgreSQLraw(sql)
	if rows != nil {
		for rows.Next() {
			rows.Scan(&threadId)
		}
		rows.Close()
	}
	if err != nil {
		// log.Error(err, s.threadID)
		log.Errorf("con:%d thread id:%d %v", s.sessionVars.ConnectionID, s.threadID, err)
		if myErr, ok := err.(*pq.Error); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err.Error())
		}
	}

	// thread_id溢出处理
	if threadId > math.MaxUint32 {
		s.threadID = uint32(threadId % (1 << 32))
	} else {
		s.threadID = uint32(threadId)
	}

	return s.threadID
}

func (s *session) PostgreSQLcreateStatisticsTable() {
	sql := "create schema if not exists inception;"
	if err := s.pgbackupdb.Exec(sql).Error; err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*pgDriver.Error); ok {
			if myErr.Code != "42P06" { /*duplicate_schema */
				s.appendErrorMsg(myErr.Message)
			}
		} else {
			s.appendErrorMsg(err.Error())
		}
	}

	sql = PostgreSQLstatisticsTableSQL()
	if err := s.pgbackupdb.Exec(sql).Error; err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*pgDriver.Error); ok {
			if myErr.Code != "42P07" { /*duplicate_table*/
				s.appendErrorMsg(myErr.Message)
			}
		} else {
			s.appendErrorMsg(err.Error())
		}
	}
}

func PostgreSQLstatisticsTableSQL() string {

	buf := bytes.NewBufferString("CREATE TABLE if not exists ")

	buf.WriteString("inception.statistic")
	buf.WriteString("(")

	buf.WriteString("id serial primary key, ")
	buf.WriteString("optime timestamp not null default current_timestamp, ")
	buf.WriteString("usedb int not null default 0, ")
	buf.WriteString("deleting int not null default 0, ")
	buf.WriteString("inserting int not null default 0, ")
	buf.WriteString("updating int not null default 0, ")
	buf.WriteString("selecting int not null default 0, ")
	buf.WriteString("altertable int not null default 0, ")
	buf.WriteString("renaming int not null default 0, ")
	buf.WriteString("createindex int not null default 0, ")
	buf.WriteString("dropindex int not null default 0, ")
	buf.WriteString("addcolumn int not null default 0, ")
	buf.WriteString("dropcolumn int not null default 0, ")
	buf.WriteString("changecolumn int not null default 0, ")
	buf.WriteString("alteroption int not null default 0, ")
	buf.WriteString("alterconvert int not null default 0, ")
	buf.WriteString("createtable int not null default 0, ")
	buf.WriteString("createview int not null default 0, ")
	buf.WriteString("droptable int not null default 0, ")
	buf.WriteString("createdb int not null default 0, ")
	buf.WriteString("truncating int not null default 0, ")
	buf.WriteString("createproc int not null default 0, ")
	buf.WriteString("dropproc int not null default 0, ")
	buf.WriteString("createfunc int not null default 0, ")
	buf.WriteString("dropfunc int not null default 0, ")
	buf.WriteString("host varchar(60) not null, ")
	buf.WriteString("port int not null);")

	return buf.String()
}

func (s *session) PostgreSQLcheckBackupdb() {
	if err := s.pgbackupdb.DB().Ping(); err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)

		addr := fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=disable search_path=inception",
			s.opt.User, s.opt.Password, s.opt.Host, s.opt.Port, s.opt.db)
		db, err := gorm.Open("postgres", addr)
		if err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			s.appendErrorMsg(err.Error())
			return
		}
		// 禁用日志记录器，不显示任何日志
		db.LogMode(false)
		s.pgbackupdb = db
	}
}
