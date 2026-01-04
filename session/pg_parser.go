package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	pgDriver "github.com/lib/pq"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

func (s *session) PostgreSQLprocessChan(wg *sync.WaitGroup) {
	for {
		r := <-s.ch
		if r == nil {
			// log.Info("剩余ch", len(s.ch), "cap ch", cap(s.ch), "通道关闭,跳出循环")
			// log.Info("ProcessChan,close")
			s.PostgreSQLflush(s.lastBackupTable, s.myRecord)
			wg.Done()
			break
		}
		// flush标志. 不能在外面调用flush函数,会导致线程并发操作,写入数据错误
		// 如数据尚未进入到ch通道,此时调用flush,数据无法正确入库

		if len(r.sqlStr) > 0 {
			s.PostgreSQLmyWriteDDL(r.sqlStr, r.opid, r.table, r.record)
		} else if len(r.sql) == 0 {
			// log.Info("flush标志")
			s.PostgreSQLflush(r.table, r.record)
		} else {
			s.PostgreSQLmyWrite(r.sql, r.opid, r.table, r.record)
		}
	}
}

func (s *session) PostgreSQLmyWriteDDL(sql string, opid string, table string, record *Record) {
	s.insertBuffer = append(s.insertBuffer, sql, opid)

	if len(s.insertBuffer) >= 1000 {
		s.PostgreSQLflush(table, record)
	}
}

func (s *session) PostgreSQLflush(table string, record *Record) {
	// log.Info("flush ", len(s.insertBuffer))
	if len(s.insertBuffer) > 0 {

		const rowSQL = "(?,?),"
		sql := "insert into %s(rollback_statement,opid_time) values%s"
		values := strings.TrimRight(strings.Repeat(rowSQL, len(s.insertBuffer)/2), ",")
		sql = fmt.Sprintf(sql, table, values)
		err := s.pgbackupdb.Exec(sql,
			s.insertBuffer...).Error
		if err != nil {
			record.StageStatus = StatusBackupFail
			if myErr, ok := err.(*pgDriver.Error); ok {
				record.appendErrorMessage(myErr.Message)
			} else {
				s.appendErrorMsg(err.Error())
			}
			log.Errorf("con:%d %v sql:%s params:%v",
				s.sessionVars.ConnectionID, err, sql, s.insertBuffer)
		}
		s.backupTotalRows += len(s.insertBuffer) / 2
		s.SetMyProcessInfo(record.Sql, time.Now(),
			float64(s.backupTotalRows)/float64(s.totalChangeRows))
	}
	s.insertBuffer = nil
}

func (s *session) PostgreSQLmyWrite(b []byte,
	opid string, table string, record *Record) {

	b = append(b, ";"...)

	s.insertBuffer = append(s.insertBuffer, string(b), opid)

	if len(s.insertBuffer) >= 1000 {
		s.PostgreSQLflush(table, record)
	}
}

func (s *session) PostgreSQLgetNextBackupRecord() *Record {
	for {
		r := s.recordSets.Next()
		if r == nil {
			return nil
		}

		if r.TableInfo != nil {

			lastBackupTable := fmt.Sprintf("\"%s\".%s", s.getRemoteBackupDBName(r), r.TableInfo.Name)

			if s.lastBackupTable == "" {
				s.lastBackupTable = lastBackupTable
			}

			if s.checkSqlIsDDL(r) {
				if s.lastBackupTable != lastBackupTable {
					s.ch <- &chanData{sql: nil, table: s.lastBackupTable, record: s.myRecord}
					s.lastBackupTable = lastBackupTable
				}

				s.ch <- &chanData{sqlStr: r.DDLRollback, opid: r.OPID,
					table: s.lastBackupTable, record: r}

				if r.StageStatus != StatusExecFail {
					r.StageStatus = StatusBackupOK
				}

				continue

			} else if (r.AffectedRows > 0 || r.StageStatus == StatusExecFail) && s.checkSqlIsDML(r) {

				// if s.opt.middlewareExtend != "" {
				// 	continue
				// }

				// 当使用事务标记时，不再使用 binlog 偏移量判断是否有变更
				if r.AffectedRows == 0 {
					continue
				}

				if s.lastBackupTable != lastBackupTable {
					s.ch <- &chanData{sql: nil, table: s.lastBackupTable, record: s.myRecord}
					s.lastBackupTable = lastBackupTable
				}

				// 先置默认值为备份失败,在备份完成后置为成功
				// if r.AffectedRows > 0 {
				if r.StageStatus != StatusExecFail {
					r.StageStatus = StatusBackupFail
				}
				// 清理已删除的列
				clearDeleteColumns(r.TableInfo)
				if r.MultiTables != nil {
					for _, t := range r.MultiTables {
						clearDeleteColumns(t)
					}
				}

				return r
			}

		} else if r.SequencesInfo != nil {

			lastBackupTable := fmt.Sprintf("\"%s\".%s", s.getRemoteBackupDBName(r), r.SequencesInfo.Name)

			if s.lastBackupTable == "" {
				s.lastBackupTable = lastBackupTable
			}

			if s.checkSqlIsDDL(r) {
				if s.lastBackupTable != lastBackupTable {
					s.ch <- &chanData{sql: nil, table: s.lastBackupTable, record: s.myRecord}
					s.lastBackupTable = lastBackupTable
				}

				s.ch <- &chanData{sqlStr: r.DDLRollback, opid: r.OPID,
					table: s.lastBackupTable, record: r}

				if r.StageStatus != StatusExecFail {
					r.StageStatus = StatusBackupOK
				}

				continue

			}
		}
	}
}

type OldKeys struct {
	KeyNames  []string      `json:"keynames"`
	KeyTypes  []string      `json:"keytypes"`
	KeyValues []interface{} `json:"keyvalues"`
}

type Change struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames,omitempty"`  // Only present in INSERT/UPDATE
	ColumnTypes  []string      `json:"columntypes,omitempty"`  // Only present in INSERT/UPDATE
	ColumnValues []interface{} `json:"columnvalues,omitempty"` // Only present in INSERT/UPDATE
	OldKeys      *OldKeys      `json:"oldkeys,omitempty"`      // Present in DELETE and UPDATE
}

type SlotChanges struct {
	Change []Change `json:"change"`
}

type Restult struct {
	Data string `gorm:"Column:data"`
}

func (s *session) PostgreSQLparserBinlog(ctx context.Context) {

	// var err error
	var wg sync.WaitGroup

	wg.Add(1)
	s.ch = make(chan *chanData, 50)
	go s.PostgreSQLprocessChan(&wg)

	// 最终关闭和返回
	defer func() {
		close(s.ch)
		wg.Wait()

		// log.Info("操作完成", "rows", i)
		// kwargs := map[string]interface{}{"ok": "1"}
		// sendMsg(p.cfg.SocketUser, "rollback_binlog_parse_complete", "binlog解析进度", "", kwargs)
	}()

	// 获取binlog解析起点
	record := s.PostgreSQLgetNextBackupRecord()
	if record == nil {
		return
	}

	s.myRecord = record

	log.Debug("Parser")
	startTime := time.Now()
	sql := fmt.Sprintf("select data from pg_logical_slot_peek_changes('%s', null, null) where xid=%d;", logicalPlugin, record.TxID)
	s.lastBackupTable = fmt.Sprintf("\"%s\".%s", record.BackupDBName, record.TableInfo.Name)

	var results []Restult

	_, err := s.PostgreSQLrawScan(sql, &results)
	if err != nil {
		log.Errorf(err.Error())
	}

	var change SlotChanges
	for _, row := range results {
		err = json.Unmarshal([]byte(row.Data), &change)
		if err != nil {
			continue
		}
		for _, col := range change.Change {
			switch col.Kind {
			case "insert":
				err := s.PostgreSQLgenerateDeleteSql(col)
				if err == nil {
					goto ENDCHECK
				}
			case "delete":
				err := s.PostgreSQLgenerateInsertSql(col)
				if err == nil {
					goto ENDCHECK
				}
			case "update":
				err := s.PostgreSQLgenerateUpdateSql(col)
				if err == nil {
					goto ENDCHECK
				}
			}
		}
	ENDCHECK:
		if record.AffectedRows > 0 {
			if int64(len(change.Change)) >= record.AffectedRows {
				record.StageStatus = StatusBackupOK
			}
		}
		record.BackupCostTime = fmt.Sprintf("%.3f", time.Since(startTime).Seconds())
		s.clearLogicalPlugin(false)
	}
}

func (s *session) PostgreSQLgenerateDeleteSql(change Change) (err error) {
	if len(change.ColumnNames) != len(change.ColumnValues) {
		log.Debug("Error: Column names and values count mismatch")
	}

	// Build WHERE clause using all column names and their values
	whereParts := make([]string, len(change.ColumnNames))
	for i, colName := range change.ColumnNames {
		value := change.ColumnValues[i]
		var valueStr string

		// Format the value based on its type
		switch v := value.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // Escape single quotes
		case int, int32, int64, float64, float32:
			valueStr = fmt.Sprintf("%v", v)
		default:
			valueStr = fmt.Sprintf("'%v'", v) // Default to string with quotes
		}

		whereParts[i] = fmt.Sprintf("%s = %s", colName, valueStr)
	}

	whereClause := strings.Join(whereParts, " AND ")
	newSql := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", change.Schema, change.Table, whereClause)
	r, err := interpolateParams(newSql, nil, s.inc.HexBlob)
	if err != nil {
		log.Error(err)
	}

	s.PostgreSQLwrite(r)
	return
}

func (s *session) PostgreSQLgenerateInsertSql(change Change) (err error) {
	if change.OldKeys == nil {
		return errors.Errorf("Error: OldKeys is missing for DELETE operation")
	}

	if len(change.OldKeys.KeyNames) != len(change.OldKeys.KeyValues) {
		return errors.Errorf("Error: Key names and values count mismatch")
	}

	// Build column names and values parts
	columns := strings.Join(change.OldKeys.KeyNames, ", ")
	valuesParts := make([]string, len(change.OldKeys.KeyValues))
	for i, value := range change.OldKeys.KeyValues {
		var valueStr string

		// Format the value based on its type
		switch v := value.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // Escape single quotes
		case int, int32, int64, float64, float32:
			valueStr = fmt.Sprintf("%v", v)
		default:
			valueStr = fmt.Sprintf("'%v'", v) // Default to string with quotes
		}

		valuesParts[i] = valueStr
	}

	valuesClause := strings.Join(valuesParts, ", ")
	newSql := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", change.Schema, change.Table, columns, valuesClause)
	r, err := interpolateParams(newSql, nil, s.inc.HexBlob)
	if err != nil {
		log.Error(err)
	}

	s.PostgreSQLwrite(r)
	return
}

func (s *session) PostgreSQLgenerateUpdateSql(change Change) (err error) {
	if len(change.ColumnNames) != len(change.ColumnValues) {
		return errors.Errorf("Error: Column names and values count mismatch")
	}

	if change.OldKeys == nil {
		return errors.Errorf("Error: OldKeys is missing for UPDATE operation")
	}

	if len(change.OldKeys.KeyNames) != len(change.OldKeys.KeyValues) {
		return errors.Errorf("Error: OldKeys names and values count mismatch")
	}

	// Build SET clause with old values (what we want to revert to)
	setParts := make([]string, len(change.OldKeys.KeyNames))
	for i, colName := range change.OldKeys.KeyNames {
		value := change.OldKeys.KeyValues[i]
		var valueStr string

		// Format the value based on its type
		switch v := value.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // Escape single quotes
		case int, int32, int64, float64, float32:
			valueStr = fmt.Sprintf("%v", v)
		default:
			valueStr = fmt.Sprintf("'%v'", v) // Default to string with quotes
		}

		setParts[i] = fmt.Sprintf("%s = %s", colName, valueStr)
	}

	setClause := strings.Join(setParts, ", ")

	// Build WHERE clause using the new values (the current state after update)
	whereParts := make([]string, len(change.ColumnNames))
	for i, colName := range change.ColumnNames {
		value := change.ColumnValues[i]
		var valueStr string

		// Format the value based on its type
		switch v := value.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")) // Escape single quotes
		case int, int32, int64, float64, float32:
			valueStr = fmt.Sprintf("%v", v)
		default:
			valueStr = fmt.Sprintf("'%v'", v) // Default to string with quotes
		}

		whereParts[i] = fmt.Sprintf("%s = %s", colName, valueStr)
	}

	whereClause := strings.Join(whereParts, " AND ")

	newSql := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", change.Schema, change.Table, setClause, whereClause)
	r, err := interpolateParams(newSql, nil, s.inc.HexBlob)
	if err != nil {
		log.Error(err)
	}

	s.PostgreSQLwrite(r)
	return
}

func (s *session) PostgreSQLwrite(b []byte) {
	// 此处执行状态不确定的记录
	if s.myRecord.StageStatus == StatusExecFail {
		log.Info("auto fix record:", s.myRecord.OPID)
		s.myRecord.AffectedRows += 1
		s.totalChangeRows += 1
	}

	s.ch <- &chanData{sql: b, e: nil, opid: s.myRecord.OPID,
		table: s.lastBackupTable, record: s.myRecord}
}
