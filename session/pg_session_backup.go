package session

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"

	pgDriver "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

func (s *session) PostgreSQLCreateBackupTable(record *Record) (
	longDataType bool, hostMaxLength int) {

	if record.TableInfo == nil {
		return
	}

	backupDBName := s.getRemoteBackupDBName(record)
	if backupDBName == "" {
		return
	}

	if record.TableInfo.IsCreated {
		// 返回longDataType值
		key := fmt.Sprintf("%s.%s", backupDBName, remoteBackupTable)
		if cache, ok := s.backupTableCacheList[key]; ok {
			return cache.longDataType, cache.hostMaxLength
		}
		return
	}

	if _, ok := s.backupDBCacheList[backupDBName]; !ok {
		sql := fmt.Sprintf("create schema if not exists \"%s\";", backupDBName)
		if err := s.backupdb.Exec(sql).Error; err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			if myErr, ok := err.(*pgDriver.Error); ok {
				if myErr.Code != "42P06" { /*duplicate_schema*/
					s.appendErrorMsg(myErr.Message)
					return
				}
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		}
		s.backupDBCacheList[backupDBName] = true
	}

	key := fmt.Sprintf("%s.%s", backupDBName, record.TableInfo.Name)
	if _, ok := s.backupTableCacheList[key]; !ok {
		createSql := s.PostgreSQLCreateSqlFromTableInfo(backupDBName, record.TableInfo)
		if err := s.backupdb.Exec(createSql).Error; err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			if myErr, ok := err.(*pgDriver.Error); ok {
				if myErr.Code != "42P07" { /*duplicate_table*/
					s.appendErrorMsg(myErr.Message)
					return
				}
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		}
		s.backupTableCacheList[key] = BackupTable{
			longDataType:  true,
			hostMaxLength: backupTableHostDataLength,
		}
	}

	key = fmt.Sprintf("%s.%s", backupDBName, remoteBackupTable)
	if _, ok := s.backupTableCacheList[key]; !ok {
		createSql := s.PostgreSQLCreateSqlBackupTable(backupDBName)
		if err := s.backupdb.Exec(createSql).Error; err != nil {
			if myErr, ok := err.(*pgDriver.Error); ok {
				if myErr.Code != "42P07" { /*ER_TABLE_EXISTS_ERROR*/
					log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
					s.appendErrorMsg(myErr.Message)
					return
				}
				// 获取sql_statement字段类型,用以兼容类型为text的旧表结构
				longDataType = s.PostgreSQLcheckBackupTableSqlStmtColumnType(backupDBName)
				// host字段长度
				hostMaxLength = s.PostgreSQLcheckBackupTableHostMaxLength(backupDBName)
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		} else {
			longDataType = true
			hostMaxLength = backupTableHostDataLength
		}
		s.backupTableCacheList[key] = BackupTable{
			longDataType:  longDataType,
			hostMaxLength: hostMaxLength,
		}
	}

	// 从remoteBackupTable表获取cache
	if cache, ok := s.backupTableCacheList[key]; ok {
		longDataType = cache.longDataType
		hostMaxLength = cache.hostMaxLength
	}
	record.TableInfo.IsCreated = true
	return
}

func (s *session) PostgreSQLcheckBackupTableSqlStmtColumnType(dbname string) (longDataType bool) {

	// 获取sql_statement字段类型,用以兼容类型为text的旧表结构
	sql := fmt.Sprintf(`select udt_name AS DATA_TYPE from information_schema.columns
					where table_schema='%s' and table_name='%s' and column_name='sql_statement';`,
		dbname, remoteBackupTable)

	var res string

	rows, err2 := s.backupdb.DB().Query(sql)
	if err2 != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err2)
		if myErr, ok := err2.(*pgDriver.Error); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err2.Error())
		}
	}
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			_ = rows.Scan(&res)
		}
		return res != "text"
	}

	return

}

func (s *session) PostgreSQLcheckBackupTableHostMaxLength(dbname string) (length int) {

	// 获取sql_statement字段类型,用以兼容类型为text的旧表结构
	sql := fmt.Sprintf(`select CONCAT(udt_name ,'(',character_maximum_length,')') COLUMN_TYPE from information_schema.columns
					where table_schema='%s' and table_name='%s' and column_name='host';`,
		dbname, remoteBackupTable)

	var res string

	rows, err2 := s.backupdb.DB().Query(sql)
	if err2 != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err2)
		if myErr, ok := err2.(*pgDriver.Error); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err2.Error())
		}
	}
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			_ = rows.Scan(&res)
		}
		if res != "" {
			typeLength := GetDataTypeLength(res)
			if len(typeLength) > 0 {
				return typeLength[0]
			}
		}
		return 0
	}

	return 0

}

func (s *session) PostgreSQLCreateSqlFromTableInfo(dbname string, ti *TableInfo) string {
	buf := bytes.NewBufferString("CREATE TABLE if not exists ")
	buf.WriteString(fmt.Sprintf("\"%s\".%s", dbname, ti.Name))
	buf.WriteString("(")

	buf.WriteString("id serial primary key, ")
	buf.WriteString("rollback_statement text, ")
	buf.WriteString("opid_time varchar(50));")

	return buf.String()
}

func (s *session) PostgreSQLCreateSqlBackupTable(dbname string) string {

	// if not exists
	buf := bytes.NewBufferString("CREATE TABLE  ")

	buf.WriteString(fmt.Sprintf("\"%s\".\"%s\"", dbname, remoteBackupTable))
	buf.WriteString("(")

	buf.WriteString("opid_time varchar(50),")
	buf.WriteString("start_binlog_file varchar(512),")
	buf.WriteString("start_binlog_pos int,")
	buf.WriteString("end_binlog_file varchar(512),")
	buf.WriteString("end_binlog_pos int,")
	buf.WriteString("sql_statement text,")
	buf.WriteString("host VARCHAR(")
	buf.WriteString(strconv.Itoa(backupTableHostDataLength))
	buf.WriteString("),")
	buf.WriteString("dbname VARCHAR(64),")
	buf.WriteString("tablename VARCHAR(64),")
	buf.WriteString("port INT,")
	buf.WriteString("time TIMESTAMP,")
	buf.WriteString("type VARCHAR(20),")
	buf.WriteString("PRIMARY KEY(opid_time));")

	return buf.String()
}

func (s *session) PostgreSQLprocessChanBackup(wg *sync.WaitGroup) {
	for {
		r := <-s.chBackupRecord

		if r == nil {
			s.PostgreSQLflushBackupRecord(s.lastBackupTable, s.myRecord)
			wg.Done()
			break
		}
		// flush标志. 不能在外面调用flush函数,会导致线程并发操作,写入数据错误
		// 如数据尚未进入到ch通道,此时调用flush,数据无法正确入库
		if r.values == nil {
			s.PostgreSQLflushBackupRecord(r.dbname, r.record)
		} else {
			s.PostgreSQLwriteBackupRecord(r.dbname, r.record, r.values)
		}
	}
}

func (s *session) PostgreSQLflushBackupRecord(dbname string, record *Record) {
	// log.Info("flush ", len(s.insertBuffer))

	if len(s.insertBuffer) > 0 {
		const backupRecordColumnCount int = 11
		const rowSQL = "(?,?,?,?,?,?,?,?,?,?,NOW(),?),"
		tableName := fmt.Sprintf("\"%s\".\"%s\"", dbname, remoteBackupTable)

		sql := "insert into %s values%s"
		values := strings.TrimRight(
			strings.Repeat(rowSQL, len(s.insertBuffer)/backupRecordColumnCount), ",")

		err := s.backupdb.Exec(fmt.Sprintf(sql, tableName, values),
			s.insertBuffer...).Error
		if err != nil {
			log.Error(err)
			if myErr, ok := err.(*pgDriver.Error); ok {
				s.recordSets.MaxLevel = 2
				record.StageStatus = StatusBackupFail
				record.appendErrorMessage(myErr.Message)
			}
		}

		// s.BackupTotalRows += len(s.insertBuffer) / backupRecordColumnCount
		// s.SetMyProcessInfo(record.Sql, time.Now(),
		//     float64(s.BackupTotalRows)/float64(s.TotalChangeRows))

		s.insertBuffer = nil
	}
}

func (s *session) PostgreSQLwriteBackupRecord(dbname string, record *Record, values []interface{}) {

	s.insertBuffer = append(s.insertBuffer, values...)

	// 每500行insert提交一次
	if len(s.insertBuffer) >= 500*11 {
		s.flushBackupRecord(dbname, record)
	}
}
