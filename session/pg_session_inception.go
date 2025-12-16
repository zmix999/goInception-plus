package session

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"gitee.com/zhoujin826/goInception-plus/parser/ast"
	"github.com/lib/pq"
	pgDriver "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

func (s *session) PostgreSqlServerVersion() {
	log.Debug("PostgreSqlServerVersion")

	if s.dbVersion > 0 {
		return
	}

	sql := "SELECT current_setting('server_version');"

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err == nil {
		s.dbType = DBPostgreSQL
	}
}

func (s *session) PgGetTableFromCache(db string, tableName string, indexName string, reportNotExists bool) *TableInfo {
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

	rows := s.PgQueryTableFromDB(db, tableName, reportNotExists)
	if rows != nil {
		newT := &TableInfo{
			Schema: db,
			Name:   tableName,
			Fields: rows,
		}
		if rows := s.PgQueryIndexFromDB(db, indexName, reportNotExists); rows != nil {
			newT.Indexes = rows
		}
		s.tableCacheList[key] = newT
		return newT
	}
	index := s.PgQueryIndexFromDB(db, indexName, reportNotExists)
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

func (s *session) PgQueryTableFromDB(db string, tableName string, reportNotExists bool) []FieldInfo {
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

func (s *session) PgQueryIndexFromDB(db string, tableName string, reportNotExists bool) []*IndexInfo {
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

func (s *session) pgCheckDBExists(db string, reportNotExists bool) bool {

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

	rows, err := s.raw(fmt.Sprintf(sql, db))
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

func (s *session) PgExecuteRemoteStatement(record *Record, isTran bool) {
	log.Debug("executeRemoteStatement")

	sqlStmt := record.Sql

	start := time.Now()

	var res sql.Result
	var err error
	if isTran {
		res, err = s.pgExecDDL(sqlStmt, false)
	} else {
		res, err = s.pgExecSQL(sqlStmt, false)
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

				record.ThreadId = s.PgFetchThreadID()
				record.ExecComplete = true
			} else {
				s.appendErrorMsg("The execution result is unknown! Please confirm manually.")
			}
		}

		// log.Infof("[%s] [%d] %s,RowsAffected: %d", s.DBName, s.fetchThreadID(), record.Sql, record.AffectedRows)

		return
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		s.appendErrorMsg(err.Error())
	}
	record.AffectedRows = affectedRows
	record.ThreadId = s.PgFetchThreadID()
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
		record.TableInfo = s.getTableFromCache(record.DBName, record.TableName, true)
	} else if _, ok := record.Type.(*ast.CreateSequenceStmt); ok &&
		record.SequencesInfo == nil && record.DBName != "" && record.TableName != "" {
		record.SequencesInfo = s.getSequencesFromCache(record.DBName, record.TableName, true)
	}
}

func (s *session) PgFetchThreadID() uint32 {

	if s.threadID > 0 {
		return s.threadID
	}

	var threadId uint64
	sql := "SELECT pg_backend_pid();"
	if s.isMiddleware() {
		sql = s.opt.middlewareExtend + sql
	}

	rows, err := s.raw(sql)
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
