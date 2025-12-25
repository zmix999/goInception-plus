// Copyright 2018 PingCAP, Inc.
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

package server

import (
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"time"

	"gitee.com/zhoujin826/goInception-plus/domain"
	"gitee.com/zhoujin826/goInception-plus/kv"
	"gitee.com/zhoujin826/goInception-plus/session"
	"gitee.com/zhoujin826/goInception-plus/sessionctx/binloginfo"
	"gitee.com/zhoujin826/goInception-plus/sessionctx/stmtctx"
	"gitee.com/zhoujin826/goInception-plus/store/helper"
	"gitee.com/zhoujin826/goInception-plus/store/mockstore"
	"gitee.com/zhoujin826/goInception-plus/tablecodec"
	"gitee.com/zhoujin826/goInception-plus/types"
	"gitee.com/zhoujin826/goInception-plus/util/codec"
	. "github.com/pingcap/check"
	"github.com/tikv/client-go/v2/tikv"
)

type basicHTTPHandlerTestSuite struct {
	*testServerClient
	server  *Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *TiDBDriver
	sh      *StatsHandler
}

type HTTPHandlerTestSuite struct {
	*basicHTTPHandlerTestSuite
}

type HTTPHandlerTestSerialSuite struct {
	*basicHTTPHandlerTestSuite
}

var _ = Suite(&HTTPHandlerTestSuite{&basicHTTPHandlerTestSuite{}})

var _ = SerialSuites(&HTTPHandlerTestSerialSuite{&basicHTTPHandlerTestSuite{}})

func (ts *basicHTTPHandlerTestSuite) SetUpSuite(c *C) {
	ts.testServerClient = newTestServerClient()
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRange(c *C) {
	sTableID := int64(3)
	sIndex := int64(11)
	eTableID := int64(9)
	recordID := int64(133)
	indexValues := []types.Datum{
		types.NewIntDatum(100),
		types.NewBytesDatum([]byte("foobar")),
		types.NewFloat64Datum(-100.25),
	}
	expectIndexValues := make([]string, 0, len(indexValues))
	for _, v := range indexValues {
		str, err := v.ToString()
		if err != nil {
			str = fmt.Sprintf("%d-%v", v.Kind(), v.GetValue())
		}
		expectIndexValues = append(expectIndexValues, str)
	}
	encodedValue, err := codec.EncodeKey(&stmtctx.StatementContext{TimeZone: time.Local}, nil, indexValues...)
	c.Assert(err, IsNil)

	startKey := tablecodec.EncodeIndexSeekKey(sTableID, sIndex, encodedValue)
	recordPrefix := tablecodec.GenTableRecordPrefix(eTableID)
	endKey := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))

	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IndexID, Equals, sIndex)
	c.Assert(r.First.IsRecord, IsFalse)
	c.Assert(r.First.RecordID, Equals, int64(0))
	c.Assert(r.First.IndexValues, DeepEquals, expectIndexValues)
	c.Assert(r.Last.RecordID, Equals, recordID)
	c.Assert(r.Last.IndexValues, IsNil)

	testCases := []struct {
		tableID int64
		indexID int64
		isCover bool
	}{
		{2, 0, false},
		{3, 0, true},
		{9, 0, true},
		{10, 0, false},
		{2, 10, false},
		{3, 10, false},
		{3, 11, true},
		{3, 20, true},
		{9, 10, true},
		{10, 1, false},
	}
	for _, t := range testCases {
		var f *helper.FrameItem
		if t.indexID == 0 {
			f = r.GetRecordFrame(t.tableID, "", "", false)
		} else {
			f = r.GetIndexFrame(t.tableID, t.indexID, "", "", "")
		}
		if t.isCover {
			c.Assert(f, NotNil)
		} else {
			c.Assert(f, IsNil)
		}
	}
}

func (ts *HTTPHandlerTestSuite) TestRegionCommonHandleRange(c *C) {
	sTableID := int64(3)
	indexValues := []types.Datum{
		types.NewIntDatum(100),
		types.NewBytesDatum([]byte("foobar")),
		types.NewFloat64Datum(-100.25),
	}
	expectIndexValues := make([]string, 0, len(indexValues))
	for _, v := range indexValues {
		str, err := v.ToString()
		if err != nil {
			str = fmt.Sprintf("%d-%v", v.Kind(), v.GetValue())
		}
		expectIndexValues = append(expectIndexValues, str)
	}
	encodedValue, err := codec.EncodeKey(&stmtctx.StatementContext{TimeZone: time.Local}, nil, indexValues...)
	c.Assert(err, IsNil)

	startKey := tablecodec.EncodeRowKey(sTableID, encodedValue)

	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   nil,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsTrue)
	c.Assert(r.First.RecordID, Equals, int64(0))
	c.Assert(r.First.IndexValues, DeepEquals, expectIndexValues)
	c.Assert(r.First.IndexName, Equals, "PRIMARY")
	c.Assert(r.Last.RecordID, Equals, int64(0))
	c.Assert(r.Last.IndexValues, IsNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRangeWithEndNoLimit(c *C) {
	sTableID := int64(15)
	startKey := tablecodec.GenTableRecordPrefix(sTableID)
	endKey := []byte("z_aaaaafdfd")
	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsTrue)
	c.Assert(r.Last.IsRecord, IsTrue)
	c.Assert(r.GetRecordFrame(300, "", "", false), NotNil)
	c.Assert(r.GetIndexFrame(200, 100, "", "", ""), NotNil)
}

func (ts *HTTPHandlerTestSuite) TestRegionIndexRangeWithStartNoLimit(c *C) {
	eTableID := int64(9)
	startKey := []byte("m_aaaaafdfd")
	endKey := tablecodec.GenTableRecordPrefix(eTableID)
	region := &tikv.KeyLocation{
		Region:   tikv.RegionVerID{},
		StartKey: startKey,
		EndKey:   endKey,
	}
	r, err := helper.NewRegionFrameRange(region)
	c.Assert(err, IsNil)
	c.Assert(r.First.IsRecord, IsFalse)
	c.Assert(r.Last.IsRecord, IsTrue)
	c.Assert(r.GetRecordFrame(3, "", "", false), NotNil)
	c.Assert(r.GetIndexFrame(8, 1, "", "", ""), NotNil)
}

func (ts *HTTPHandlerTestSuite) TestBinlogRecover(c *C) {
	ts.startServer(c)
	defer ts.stopServer(c)
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err := ts.fetchStatus("/binlog/recover")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Invalid operation will use the default operation.
	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = ts.fetchStatus("/binlog/recover?op=abc")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	binloginfo.EnableSkipBinlogFlag()
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, true)
	binloginfo.AddOneSkippedCommitter()
	resp, err = ts.fetchStatus("/binlog/recover?op=abc&seconds=1")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusBadRequest)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)
	binloginfo.RemoveOneSkippedCommitter()

	binloginfo.AddOneSkippedCommitter()
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(1))
	resp, err = ts.fetchStatus("/binlog/recover?op=reset")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.SkippedCommitterCount(), Equals, int32(0))

	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	// Only the first should work.
	binloginfo.EnableSkipBinlogFlag()
	resp, err = ts.fetchStatus("/binlog/recover?op=nowait&op=reset")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
	c.Assert(binloginfo.IsBinlogSkipped(), Equals, false)

	resp, err = ts.fetchStatus("/binlog/recover?op=status")
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}

func (ts *basicHTTPHandlerTestSuite) startServer(c *C) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)

	cfg := newTestConfig()
	cfg.Store = "tikv"
	cfg.Port = 0
	cfg.Status.StatusPort = 0
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.port = getPortFromTCPAddr(server.listener.Addr())
	ts.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ts.server = server
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	ts.waitUntilServerOnline()

	do, err := session.GetDomain(ts.store)
	c.Assert(err, IsNil)
	ts.sh = &StatsHandler{do}
}

func getPortFromTCPAddr(addr net.Addr) uint {
	return uint(addr.(*net.TCPAddr).Port)
}

func (ts *basicHTTPHandlerTestSuite) stopServer(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
	if ts.domain != nil {
		ts.domain.Close()
	}
	if ts.store != nil {
		ts.store.Close()
	}
}

func (ts *basicHTTPHandlerTestSuite) prepareData(c *C) {
	db, err := sql.Open("mysql", ts.getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer func() {
		err := db.Close()
		c.Assert(err, IsNil)
	}()
	dbt := &DBTest{c, db}

	dbt.mustExec("create database tidb;")
	dbt.mustExec("use tidb;")
	dbt.mustExec("create table tidb.test (a int auto_increment primary key, b varchar(20));")
	dbt.mustExec("insert tidb.test values (1, 1);")
	txn1, err := dbt.db.Begin()
	c.Assert(err, IsNil)
	_, err = txn1.Exec("update tidb.test set b = b + 1 where a = 1;")
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert tidb.test values (2, 2);")
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert tidb.test (a) values (3);")
	c.Assert(err, IsNil)
	_, err = txn1.Exec("insert tidb.test values (4, '');")
	c.Assert(err, IsNil)
	err = txn1.Commit()
	c.Assert(err, IsNil)
	dbt.mustExec("alter table tidb.test add index idx1 (a, b);")
	dbt.mustExec("alter table tidb.test add unique index idx2 (a, b);")

	dbt.mustExec(`create table tidb.pt (a int primary key, b varchar(20), key idx(a, b))
partition by range (a)
(partition p0 values less than (256),
 partition p1 values less than (512),
 partition p2 values less than (1024))`)

	txn2, err := dbt.db.Begin()
	c.Assert(err, IsNil)
	_, err = txn2.Exec("insert into tidb.pt values (42, '123')")
	c.Assert(err, IsNil)
	_, err = txn2.Exec("insert into tidb.pt values (256, 'b')")
	c.Assert(err, IsNil)
	_, err = txn2.Exec("insert into tidb.pt values (666, 'def')")
	c.Assert(err, IsNil)
	err = txn2.Commit()
	c.Assert(err, IsNil)
	dbt.mustExec("drop table if exists t")
	dbt.mustExec("create table t (a double, b varchar(20), c int, primary key(a,b) clustered, key idx(c))")
	dbt.mustExec("insert into t values(1.1,'111',1),(2.2,'222',2)")
}
