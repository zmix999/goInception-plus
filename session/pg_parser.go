package session

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

func (s *session) PostgreSQLmyWrite(b []byte,
	opid string, table string, record *Record) {

	b = append(b, ";"...)

	s.insertBuffer = append(s.insertBuffer, string(b), opid)

	if len(s.insertBuffer) >= 1000 {
		s.flush(table, record)
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

type WalEvent struct {
	Xid    uint32   `json:"xid"`
	Change []Change `json:"change"`
}

func (s *session) parserWallog(ctx context.Context) {

	// var err error
	var wg sync.WaitGroup

	wg.Add(1)
	s.ch = make(chan *chanData, 50)
	go s.processChan(&wg)

	// 最终关闭和返回
	defer func() {
		close(s.ch)
		wg.Wait()

		// log.Info("操作完成", "rows", i)
		// kwargs := map[string]interface{}{"ok": "1"}
		// sendMsg(p.cfg.SocketUser, "rollback_binlog_parse_complete", "binlog解析进度", "", kwargs)
	}()

	record := s.getNextBackupRecord()
	if record == nil {
		return
	}

	s.myRecord = record

	log.Debug("Parser")
	startTime := time.Now()

	if s.inc.BackupDbType == "mysql" {
		s.lastBackupTable = fmt.Sprintf("`%s`.`%s`", record.BackupDBName, record.TableInfo.Name)
	} else if s.inc.BackupDbType == "postgres" {
		s.lastBackupTable = fmt.Sprintf("\"%s\".%s", record.BackupDBName, record.TableInfo.Name)
	}

	defer s.conn.Close(ctx)

	var changeRows int
	var walevent WalEvent
	relations := map[uint32]*pglogrepl.RelationMessageV2{}
	clientXLogPos := s.XLogPos
	nextStandbyMessageDeadline := time.Now().Add(time.Second * 10)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			nextStandbyMessageDeadline = time.Now().Add(time.Second * 10)
		}

		rawMsg, err := s.conn.ReceiveMessage(ctx)
		if err != nil {
			log.Infof("Get message error: %v\n", errors.ErrorStack(err))
			s.appendErrorMsg(err.Error())
			break
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Received unexpected message: %T\n", rawMsg)
			continue
		}
		switch msg.Data[0] {
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatalln("ParseXLogData failed:", err)
			}
			if s.inc.WalPlugin == "pgoutput" {
				logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
				if err != nil {
					log.Fatalf("Parse logical replication message: %s", err)
				}
				switch logicalMsg := logicalMsg.(type) {
				case *pglogrepl.RelationMessageV2:
					relations[logicalMsg.RelationID] = logicalMsg
				case *pglogrepl.BeginMessage:
					s.Xid = logicalMsg.Xid
				case *pglogrepl.InsertMessageV2:
					if s.txID == s.Xid {
						rel, ok := relations[logicalMsg.RelationID]
						if !ok {
							log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
						}
						walevent = s.ParseInsertMessage(rel, logicalMsg)
						changeRows, err = s.process(record, walevent)
						if err != nil {
							log.Error(err)
						} else {
							goto ENDCHECK
						}
					}
				case *pglogrepl.UpdateMessageV2:
					if s.txID == s.Xid {
						rel, ok := relations[logicalMsg.RelationID]
						if !ok {
							log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
						}
						walevent = s.ParseUpdateMessage(rel, logicalMsg)
						changeRows, err = s.process(record, walevent)
						if err != nil {
							log.Error(err)
						} else {
							goto ENDCHECK
						}

					}
				case *pglogrepl.DeleteMessageV2:
					if s.txID == s.Xid {
						rel, ok := relations[logicalMsg.RelationID]
						if !ok {
							log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
						}
						walevent = s.ParseDeleteMessage(rel, logicalMsg)
						changeRows, err = s.process(record, walevent)
						if err != nil {
							log.Error(err)
						} else {
							goto ENDCHECK
						}
					}
				}

			} else if s.inc.WalPlugin == "wal2json" {
				err = json.Unmarshal([]byte(xld.WALData), &walevent)
				if err != nil {
					continue
				}
				changeRows, err = s.process(record, walevent)
				if err != nil {
					log.Error(err)
				} else {
					goto ENDCHECK
				}
			}

		}
	}
ENDCHECK:
	if record.AffectedRows > 0 {
		if int64(changeRows) >= record.AffectedRows {
			record.StageStatus = StatusBackupOK
		}
	}
	record.BackupCostTime = fmt.Sprintf("%.3f", time.Since(startTime).Seconds())

	if !s.killExecute {
		if err := checkClose(ctx); err != nil {
			log.Warn("Killed: ", err)
			s.appendErrorMsg("Operation has been killed!")
		}
	}
}

func (s *session) ParseInsertMessage(rel *pglogrepl.RelationMessageV2, logicalMsg *pglogrepl.InsertMessageV2) WalEvent {
	var change Change
	var walevent WalEvent
	typeMap := pgtype.NewMap()
	for idx, col := range logicalMsg.Tuple.Columns {
		if dt, ok := typeMap.TypeForOID(rel.Columns[idx].DataType); ok {
			change.ColumnTypes = append(change.ColumnTypes, dt.Name)
		}
		change.ColumnNames = append(change.ColumnNames, rel.Columns[idx].Name)
		change.ColumnValues = append(change.ColumnValues, col.Data)
	}
	change.Kind = "insert"
	change.Schema = rel.Namespace
	change.Table = rel.RelationName
	walevent.Xid = s.Xid
	walevent.Change = append(walevent.Change, change)
	return walevent
}

func (s *session) ParseUpdateMessage(rel *pglogrepl.RelationMessageV2, logicalMsg *pglogrepl.UpdateMessageV2) WalEvent {
	var change Change
	var oldKeys OldKeys
	var walevent WalEvent
	typeMap := pgtype.NewMap()
	for idx, col := range logicalMsg.NewTuple.Columns {
		if dt, ok := typeMap.TypeForOID(rel.Columns[idx].DataType); ok {
			change.ColumnTypes = append(change.ColumnTypes, dt.Name)
		}
		change.ColumnNames = append(change.ColumnNames, rel.Columns[idx].Name)
		change.ColumnValues = append(change.ColumnValues, col.Data)
	}
	for idx, col := range logicalMsg.OldTuple.Columns {
		if dt, ok := typeMap.TypeForOID(rel.Columns[idx].DataType); ok {
			oldKeys.KeyTypes = append(oldKeys.KeyTypes, dt.Name)
		}
		oldKeys.KeyNames = append(oldKeys.KeyNames, rel.Columns[idx].Name)
		oldKeys.KeyValues = append(oldKeys.KeyValues, col.Data)
	}

	change.Kind = "update"
	change.Schema = rel.Namespace
	change.Table = rel.RelationName
	change.OldKeys = &oldKeys
	walevent.Xid = s.Xid
	walevent.Change = append(walevent.Change, change)
	return walevent
}

func (s *session) ParseDeleteMessage(rel *pglogrepl.RelationMessageV2, logicalMsg *pglogrepl.DeleteMessageV2) WalEvent {
	var change Change
	var oldKeys OldKeys
	var walevent WalEvent
	typeMap := pgtype.NewMap()
	for idx, col := range logicalMsg.OldTuple.Columns {
		if dt, ok := typeMap.TypeForOID(rel.Columns[idx].DataType); ok {
			oldKeys.KeyTypes = append(oldKeys.KeyTypes, dt.Name)
		}
		oldKeys.KeyNames = append(oldKeys.KeyNames, rel.Columns[idx].Name)
		oldKeys.KeyValues = append(oldKeys.KeyValues, col.Data)
	}
	change.Kind = "delete"
	change.OldKeys = &oldKeys
	change.Schema = rel.Namespace
	change.Table = rel.RelationName
	walevent.Xid = s.Xid
	walevent.Change = append(walevent.Change, change)
	return walevent
}

func (s *session) process(record *Record, walevent WalEvent) (int, error) {
	var err error
	var changeRows int
	if walevent.Xid == s.txID {
		for _, col := range walevent.Change {
			switch col.Kind {
			case "insert":
				err = s.PostgreSQLgenerateDeleteSql(record.TableInfo, col)
			case "delete":
				err = s.PostgreSQLgenerateInsertSql(record.TableInfo, col)
			case "update":
				err = s.PostgreSQLgenerateUpdateSql(record.TableInfo, col)
			}
			changeRows++
		}
	}
	return changeRows, err
}

func (s *session) PostgreSQLgenerateDeleteSql(t *TableInfo, change Change) (err error) {

	if len(t.Fields) < len(change.ColumnNames) {
		return errors.Errorf("表%s.%s缺少列!当前列数:%d,wal的列数%d",
			change.Schema, change.Table, len(t.Fields), len(change.ColumnNames))
	}

	template := "DELETE FROM %s.%s WHERE"

	sql := fmt.Sprintf(template, change.Schema, change.Table)

	var columnNames []string
	c_null := " %s IS ?"
	c := " %s=?"
	var vv []driver.Value

	for i, _ := range change.ColumnNames {
		value := change.ColumnValues[i]
		if t.Fields[i].isUnsigned() {
			value = processValue(value, GetDataTypeBase(t.Fields[i].Type))
		}
		vv = append(vv, value)
		if value == nil {
			columnNames = append(columnNames,
				fmt.Sprintf(c_null, t.Fields[i].Field))
		} else {
			columnNames = append(columnNames,
				fmt.Sprintf(c, t.Fields[i].Field))
		}
	}
	newSql := strings.Join([]string{sql, strings.Join(columnNames, " AND")}, "")
	r, err := interpolateParams(newSql, vv, s.inc.HexBlob)
	if err != nil {
		log.Error(err)
	}

	s.PostgreSQLwrite(r)
	return
}

func (s *session) PostgreSQLgenerateInsertSql(t *TableInfo, change Change) (err error) {
	if len(t.Fields) < len(change.ColumnNames) {
		return errors.Errorf("表%s.%s缺少列!当前列数:%d,wal的列数%d",
			change.Schema, change.Table, len(t.Fields), len(change.ColumnNames))
	}

	var vv []driver.Value
	var columnNames []string
	c := "%s"
	template := "INSERT INTO %s.%s (%s) VALUES (%s)"
	for i, col := range t.Fields {
		if i < len(change.OldKeys.KeyNames) && !col.IsGenerated() {
			columnNames = append(columnNames, fmt.Sprintf(c, col.Field))
		}
	}

	paramValues := strings.Repeat("?,", t.EffectiveFieldCount())
	paramValues = strings.TrimRight(paramValues, ",")

	sql := fmt.Sprintf(template, change.Schema, change.Table,
		strings.Join(columnNames, ","), paramValues)

	for i, value := range change.OldKeys.KeyValues {
		if t.Fields[i].IsGenerated() {
			continue
		}
		if t.Fields[i].isUnsigned() {
			value = processValue(value, GetDataTypeBase(t.Fields[i].Type))
		}
		vv = append(vv, value)
	}

	r, err := interpolateParams(sql, vv, s.inc.HexBlob)
	if err != nil {
		log.Error(err)
	}
	log.Debug(r)
	s.PostgreSQLwrite(r)
	return
}

func (s *session) PostgreSQLgenerateUpdateSql(t *TableInfo, change Change) (err error) {
	if len(t.Fields) < len(change.ColumnNames) {
		return errors.Errorf("表%s.%s缺少列!当前列数:%d,wal的列数%d",
			change.Schema, change.Table, len(t.Fields), len(change.ColumnNames))
	}

	template := "UPDATE %s.%s SET%s WHERE"

	setValue := " %s=?"

	var sql string
	var sets []string
	var columnNames []string
	c_null := " %s IS ?"
	c := " %s=?"

	for i, col := range t.Fields {
		if i < len(change.OldKeys.KeyNames) && !col.IsGenerated() {
			sets = append(sets, fmt.Sprintf(setValue, col.Field))
		}
	}
	sql = fmt.Sprintf(template, change.Schema, change.Table, strings.Join(sets, ","))

	var (
		oldValues []driver.Value
		newValues []driver.Value
		newSql    string
	)

	for i, _ := range change.ColumnNames {
		value := change.ColumnValues[i]
		if t.Fields[i].isUnsigned() {
			value = processValue(value, GetDataTypeBase(t.Fields[i].Type))
		}
		oldValues = append(oldValues, value)
	}

	for i, _ := range change.OldKeys.KeyNames {
		value := change.OldKeys.KeyValues[i]
		if t.Fields[i].isUnsigned() {
			value = processValue(value, GetDataTypeBase(t.Fields[i].Type))
		}
		newValues = append(newValues, value)

		if value == nil {
			columnNames = append(columnNames,
				fmt.Sprintf(c_null, t.Fields[i].Field))
		} else {
			columnNames = append(columnNames,
				fmt.Sprintf(c, t.Fields[i].Field))
		}
	}

	newSql = strings.Join([]string{sql, strings.Join(columnNames, " AND")}, "")
	newValues = append(newValues, oldValues...)
	r, err := interpolateParams(newSql, newValues, s.inc.HexBlob)
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
