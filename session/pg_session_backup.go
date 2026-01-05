package session

import (
	"bytes"
	"fmt"
	"strconv"

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
