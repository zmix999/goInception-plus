package session

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/lib/pq"
	pgDriver "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"github.com/zmix999/goInception-plus/parser/ast"
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

func (s *session) PostgreSQLgetTableFromCache(schema string, tableName string, indexName string, reportNotExists bool) *TableInfo {
	if schema == "" {
		schema = s.dbName
	}

	if schema == "" {
		s.appendErrorNo(ER_WRONG_SCHEMA_NAME, "")
		return nil
	}

	key := fmt.Sprintf("%s.%s", schema, tableName)
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

	rows := s.PostgreSQLqueryTableFromDB(schema, tableName, reportNotExists)
	if rows != nil {
		newT := &TableInfo{
			Schema: schema,
			Name:   tableName,
			Fields: rows,
		}
		if rows := s.PostgreSQLqueryIndexFromDB(schema, indexName, reportNotExists); rows != nil {
			newT.Indexes = rows
		}
		s.tableCacheList[key] = newT
		return newT
	}
	index := s.PostgreSQLqueryIndexFromDB(schema, indexName, reportNotExists)
	if index != nil {
		newT := &TableInfo{
			Schema: schema,
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
		db = s.dbName
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
		db = s.dbName
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

func (s *session) PostgreSQLbuildReturningSQL(record *Record) string {
	var sqlBuilder strings.Builder
	sqlBuilder.Grow(len(record.Sql) + 15) // xmin,xmax length + space
	sqlBuilder.WriteString(record.Sql)
	sqlBuilder.WriteString(" returning xmin,xmax")
	return sqlBuilder.String()
}

type ReturningValues struct {
	Xmin uint32 `gorm:"Column:xmin"`
	Xmax uint32 `gorm:"Column:xmax"`
}

func (s *session) PostgreSQLexecuteRemoteStatement(record *Record, isTran bool, isDml bool) {
	log.Debug("PostgreSQLexecuteRemoteStatement")
	var sqlStmt string
	var isUse bool
	switch record.Type.(type) {
	case *ast.UseStmt:
		isUse = true
	}
	//创建逻辑解密槽
	if !isUse && isDml {
		s.initLogicalPlugin(context.Background())
		sqlStmt = s.PostgreSQLbuildReturningSQL(record)
	} else if isUse {
		sqlStmt = fmt.Sprintf("set search_path to %s", s.dbName)
	} else {
		sqlStmt = record.Sql
	}

	start := time.Now()

	var res sql.Result
	var err error
	var affectedRows int64
	var returing ReturningValues
	if isTran {
		res, err = s.PostgreSQLexecDDL(sqlStmt, false)
	}
	if isDml {
		affectedRows, err = s.PostgreSQLrawScan(sqlStmt, &returing)
	} else {
		res, err = s.PostgreSQLexecSQL(sqlStmt, false)
	}

	record.ExecTime = fmt.Sprintf("%.3f", time.Since(start).Seconds())
	record.ExecTimestamp = time.Now().Unix()
	if !isUse {
		if returing.Xmax > 0 {
			s.txID = returing.Xmax
		} else if returing.Xmin > 0 {
			s.txID = returing.Xmin
		}
	}
	if err != nil {
		s.checkError(err)
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
	if err := s.backupdb.Exec(sql).Error; err != nil {
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
	if err := s.backupdb.Exec(sql).Error; err != nil {
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

func (s *session) PostgreSQLcheckDBExists(schema string, reportNotExists bool) bool {

	if schema == "" {
		schema = s.dbName
	}

	if schema == "" {
		s.appendErrorNo(ER_WRONG_SCHEMA_NAME, "")
		return false
	}

	key := schema
	if s.IgnoreCase() {
		key = strings.ToLower(schema)
	}
	if v, ok := s.dbCacheList[key]; ok {
		return !v.IsDeleted
	}

	sql := "select schema_name from information_schema.schemata where schema_name ='%s';"

	// count:= s.Exec(fmt.Sprintf(sql,db)).AffectedRows
	var name string

	rows, err := s.PostgreSQLraw(fmt.Sprintf(sql, schema))
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		s.checkError(err)
	} else {
		for rows.Next() {
			rows.Scan(&name)
		}
	}

	if name == "" {
		if reportNotExists {
			s.appendErrorNo(ER_SCHEMA_NOT_EXISTED_ERROR, schema)
		}
		return false
	}

	s.dbCacheList[key] = &DBInfo{
		Name:      schema,
		IsNew:     false,
		IsDeleted: false,
	}

	return true
}

// PostgreSQLShowTableStatus 获取表估计的受影响行数
func (s *session) PostgreSQLShowTableStatus(t *TableInfo) {

	if t.IsNew {
		return
	}

	sql := fmt.Sprintf(`select n_live_tup as TABLE_ROWS from pg_catalog.pg_stat_user_tables where schemaname='%s' and relname='%s';`, t.Schema, t.Name)

	var (
		res uint64
	)

	rows, err := s.PostgreSQLraw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		s.checkError(err)
	} else if rows != nil {
		for rows.Next() {
			rows.Scan(&res)
		}
		s.myRecord.AffectedRows = int64(res)
	}
}

// PostgreSQLGetTableSize 获取表的大小
func (s *session) PostgreSQLGetTableSize(t *TableInfo) {

	if t.IsNew || t.TableSize > 0 {
		return
	}

	sql := fmt.Sprintf(`select pg_total_relation_size(relid)/1024/1024 as v from pg_catalog.pg_stat_user_tables where schemaname='%s' and relname='%s';`, t.Schema, t.Name)

	var res float64

	rows, err := s.PostgreSQLraw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		s.checkError(err)
	} else if rows != nil {
		for rows.Next() {
			rows.Scan(&res)
		}
		t.TableSize = uint(res)
	}
}

func (s *session) checkWalLevelIsLogical() bool {
	log.Debug("checkWalLevel")

	sql := "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'wal_level';"

	var setting string

	rows, err := s.PostgreSQLraw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		s.checkError(err)
	} else {
		for rows.Next() {
			rows.Scan(&setting)
		}
	}
	return setting != "replica"
}

func (s *session) modifyWalLevelIsLogical() {
	log.Debug("modifyWalLevelIsLogical")

	sql := "alter system set wal_level='logical';"

	if _, err := s.PostgreSQLexecSQL(sql, true); err != nil {
		s.checkError(err)
	}
}

func (s *session) checkReplicaIdentityIsFull() bool {
	log.Debug("checkReplicaIdentityIsFull")

	sql := fmt.Sprintf("select pc.relreplident from pg_catalog.pg_class pc, pg_catalog.pg_namespace pn where pc.relnamespace = pn.oid and pn.nspname ='%s' and pc.relname ='%s';", s.myRecord.TableInfo.Schema, s.myRecord.TableInfo.Name)

	var format string

	rows, err := s.PostgreSQLraw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		s.checkError(err)
	} else {
		for rows.Next() {
			rows.Scan(&format)
		}
	}

	return format != "d"
}

func (s *session) modifyReplicaIdentityIsFull() {
	log.Debug("modifyReplicaIdentityIsFull")

	sql := fmt.Sprintf("alter table %s.%s replica identity full;", s.myRecord.TableInfo.Schema, s.myRecord.TableInfo.Name)

	if _, err := s.PostgreSQLexecSQL(sql, true); err != nil {
		s.checkError(err)
	}
}

func (s *session) initLogicalPlugin(ctx context.Context) {
	log.Debug("initLogicalPlugin")
	if s.inc.WalPlugin == "pgoutput" {
		drop_pub := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", publication)
		result := s.conn.Exec(ctx, drop_pub)
		_, err := result.ReadAll()
		if err != nil {
			log.Fatalln("drop publication if exists error", err)
		}
		create_pub := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", publication)
		result = s.conn.Exec(ctx, create_pub)
		_, err = result.ReadAll()
		if err != nil {
			log.Fatalln("create publication error", err)
		}
	}

	var pluginArguments []string
	if s.inc.WalPlugin == "pgoutput" {
		// streaming of large transactions is available since PG 14 (protocol version 2)
		// we also need to set 'streaming' to 'true'
		pluginArguments = []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", publication),
			"messages 'true'",
			"streaming 'true'",
		}
	} else if s.inc.WalPlugin == "wal2json" {
		pluginArguments = []string{"\"include-xids\" '1'"}
	}

	sysident, err := pglogrepl.IdentifySystem(ctx, s.conn)
	if err != nil {
		s.appendErrorMsg(err.Error())
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx, s.conn, logicalPlugin, s.inc.WalPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	err = pglogrepl.StartReplication(ctx, s.conn, logicalPlugin, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
}
