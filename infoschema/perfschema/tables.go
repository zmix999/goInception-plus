// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package perfschema

import (
	"strings"

	"github.com/zmix999/goInception-plus/infoschema"
	"github.com/zmix999/goInception-plus/kv"
	"github.com/zmix999/goInception-plus/meta/autoid"
	"github.com/zmix999/goInception-plus/parser/model"
	"github.com/zmix999/goInception-plus/sessionctx"
	"github.com/zmix999/goInception-plus/table"
	"github.com/zmix999/goInception-plus/table/tables"
	"github.com/zmix999/goInception-plus/types"
)

const (
	tableNameGlobalStatus                    = "global_status"
	tableNameSessionStatus                   = "session_status"
	tableNameSetupActors                     = "setup_actors"
	tableNameSetupObjects                    = "setup_objects"
	tableNameSetupInstruments                = "setup_instruments"
	tableNameSetupConsumers                  = "setup_consumers"
	tableNameEventsStatementsCurrent         = "events_statements_current"
	tableNameEventsStatementsHistory         = "events_statements_history"
	tableNameEventsStatementsHistoryLong     = "events_statements_history_long"
	tableNamePreparedStatementsInstances     = "prepared_statements_instances"
	tableNameEventsTransactionsCurrent       = "events_transactions_current"
	tableNameEventsTransactionsHistory       = "events_transactions_history"
	tableNameEventsTransactionsHistoryLong   = "events_transactions_history_long"
	tableNameEventsStagesCurrent             = "events_stages_current"
	tableNameEventsStagesHistory             = "events_stages_history"
	tableNameEventsStagesHistoryLong         = "events_stages_history_long"
	tableNameEventsStatementsSummaryByDigest = "events_statements_summary_by_digest"
	tableNameTiDBProfileCPU                  = "tidb_profile_cpu"
	tableNameTiDBProfileMemory               = "tidb_profile_memory"
	tableNameTiDBProfileMutex                = "tidb_profile_mutex"
	tableNameTiDBProfileAllocs               = "tidb_profile_allocs"
	tableNameTiDBProfileBlock                = "tidb_profile_block"
	tableNameTiDBProfileGoroutines           = "tidb_profile_goroutines"
	tableNameTiKVProfileCPU                  = "tikv_profile_cpu"
	tableNamePDProfileCPU                    = "pd_profile_cpu"
	tableNamePDProfileMemory                 = "pd_profile_memory"
	tableNamePDProfileMutex                  = "pd_profile_mutex"
	tableNamePDProfileAllocs                 = "pd_profile_allocs"
	tableNamePDProfileBlock                  = "pd_profile_block"
	tableNamePDProfileGoroutines             = "pd_profile_goroutines"
)

var tableIDMap = map[string]int64{
	tableNameGlobalStatus:                    autoid.PerformanceSchemaDBID + 1,
	tableNameSessionStatus:                   autoid.PerformanceSchemaDBID + 2,
	tableNameSetupActors:                     autoid.PerformanceSchemaDBID + 3,
	tableNameSetupObjects:                    autoid.PerformanceSchemaDBID + 4,
	tableNameSetupInstruments:                autoid.PerformanceSchemaDBID + 5,
	tableNameSetupConsumers:                  autoid.PerformanceSchemaDBID + 6,
	tableNameEventsStatementsCurrent:         autoid.PerformanceSchemaDBID + 7,
	tableNameEventsStatementsHistory:         autoid.PerformanceSchemaDBID + 8,
	tableNameEventsStatementsHistoryLong:     autoid.PerformanceSchemaDBID + 9,
	tableNamePreparedStatementsInstances:     autoid.PerformanceSchemaDBID + 10,
	tableNameEventsTransactionsCurrent:       autoid.PerformanceSchemaDBID + 11,
	tableNameEventsTransactionsHistory:       autoid.PerformanceSchemaDBID + 12,
	tableNameEventsTransactionsHistoryLong:   autoid.PerformanceSchemaDBID + 13,
	tableNameEventsStagesCurrent:             autoid.PerformanceSchemaDBID + 14,
	tableNameEventsStagesHistory:             autoid.PerformanceSchemaDBID + 15,
	tableNameEventsStagesHistoryLong:         autoid.PerformanceSchemaDBID + 16,
	tableNameEventsStatementsSummaryByDigest: autoid.PerformanceSchemaDBID + 17,
	tableNameTiDBProfileCPU:                  autoid.PerformanceSchemaDBID + 18,
	tableNameTiDBProfileMemory:               autoid.PerformanceSchemaDBID + 19,
	tableNameTiDBProfileMutex:                autoid.PerformanceSchemaDBID + 20,
	tableNameTiDBProfileAllocs:               autoid.PerformanceSchemaDBID + 21,
	tableNameTiDBProfileBlock:                autoid.PerformanceSchemaDBID + 22,
	tableNameTiDBProfileGoroutines:           autoid.PerformanceSchemaDBID + 23,
	tableNameTiKVProfileCPU:                  autoid.PerformanceSchemaDBID + 24,
	tableNamePDProfileCPU:                    autoid.PerformanceSchemaDBID + 25,
	tableNamePDProfileMemory:                 autoid.PerformanceSchemaDBID + 26,
	tableNamePDProfileMutex:                  autoid.PerformanceSchemaDBID + 27,
	tableNamePDProfileAllocs:                 autoid.PerformanceSchemaDBID + 28,
	tableNamePDProfileBlock:                  autoid.PerformanceSchemaDBID + 29,
	tableNamePDProfileGoroutines:             autoid.PerformanceSchemaDBID + 30,
}

// perfSchemaTable stands for the fake table all its data is in the memory.
type perfSchemaTable struct {
	infoschema.VirtualTable
	meta    *model.TableInfo
	cols    []*table.Column
	tp      table.Type
	indices []table.Index
}

var pluginTable = make(map[string]func(autoid.Allocators, *model.TableInfo) (table.Table, error))

// IsPredefinedTable judges whether this table is predefined.
func IsPredefinedTable(tableName string) bool {
	_, ok := tableIDMap[strings.ToLower(tableName)]
	return ok
}

// RegisterTable registers a new table into TiDB.
func RegisterTable(tableName, sql string,
	tableFromMeta func(autoid.Allocators, *model.TableInfo) (table.Table, error)) {
	perfSchemaTables = append(perfSchemaTables, sql)
	pluginTable[tableName] = tableFromMeta
}

func tableFromMeta(allocs autoid.Allocators, meta *model.TableInfo) (table.Table, error) {
	if f, ok := pluginTable[meta.Name.L]; ok {
		ret, err := f(allocs, meta)
		return ret, err
	}
	return createPerfSchemaTable(meta)
}

// createPerfSchemaTable creates all perfSchemaTables
func createPerfSchemaTable(meta *model.TableInfo) (*perfSchemaTable, error) {
	columns := make([]*table.Column, 0, len(meta.Columns))
	for _, colInfo := range meta.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}
	tp := table.VirtualTable
	t := &perfSchemaTable{
		meta: meta,
		cols: columns,
		tp:   tp,
	}
	if err := initTableIndices(t); err != nil {
		return nil, err
	}
	return t, nil
}

// Cols implements table.Table Type interface.
func (vt *perfSchemaTable) Cols() []*table.Column {
	return vt.cols
}

// VisibleCols implements table.Table VisibleCols interface.
func (vt *perfSchemaTable) VisibleCols() []*table.Column {
	return vt.cols
}

// HiddenCols implements table.Table HiddenCols interface.
func (vt *perfSchemaTable) HiddenCols() []*table.Column {
	return nil
}

// WritableCols implements table.Table Type interface.
func (vt *perfSchemaTable) WritableCols() []*table.Column {
	return vt.cols
}

// DeletableCols implements table.Table Type interface.
func (vt *perfSchemaTable) DeletableCols() []*table.Column {
	return vt.cols
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (vt *perfSchemaTable) FullHiddenColsAndVisibleCols() []*table.Column {
	return vt.cols
}

// GetPhysicalID implements table.Table GetID interface.
func (vt *perfSchemaTable) GetPhysicalID() int64 {
	return vt.meta.ID
}

// Meta implements table.Table Type interface.
func (vt *perfSchemaTable) Meta() *model.TableInfo {
	return vt.meta
}

// Type implements table.Table Type interface.
func (vt *perfSchemaTable) Type() table.Type {
	return vt.tp
}

// Indices implements table.Table Indices interface.
func (vt *perfSchemaTable) Indices() []table.Index {
	return vt.indices
}

// initTableIndices initializes the indices of the perfSchemaTable.
func initTableIndices(t *perfSchemaTable) error {
	tblInfo := t.meta
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State == model.StateNone {
			return table.ErrIndexStateCantNone.GenWithStackByArgs(idxInfo.Name)
		}
		idx := tables.NewIndex(t.meta.ID, tblInfo, idxInfo)
		t.indices = append(t.indices, idx)
	}
	return nil
}

func (vt *perfSchemaTable) getRows(ctx sessionctx.Context, cols []*table.Column) (fullRows [][]types.Datum, err error) {
	switch vt.meta.Name.O {
	case tableNameTiDBProfileCPU:
	case tableNameTiDBProfileMemory:
	case tableNameTiDBProfileMutex:
	case tableNameTiDBProfileAllocs:
	case tableNameTiDBProfileBlock:
	case tableNameTiDBProfileGoroutines:
	case tableNameTiKVProfileCPU:
	case tableNamePDProfileCPU:
	case tableNamePDProfileMemory:
	case tableNamePDProfileMutex:
	case tableNamePDProfileAllocs:
	case tableNamePDProfileBlock:
	case tableNamePDProfileGoroutines:
	}
	if err != nil {
		return
	}
	if len(cols) == len(vt.cols) {
		return
	}
	rows := make([][]types.Datum, len(fullRows))
	for i, fullRow := range fullRows {
		row := make([]types.Datum, len(cols))
		for j, col := range cols {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

// IterRecords implements table.Table IterRecords interface.
func (vt *perfSchemaTable) IterRecords(ctx sessionctx.Context, cols []*table.Column,
	fn table.RecordIterFunc) error {
	rows, err := vt.getRows(ctx, cols)
	if err != nil {
		return err
	}
	for i, row := range rows {
		more, err := fn(kv.IntHandle(i), row, cols)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}
