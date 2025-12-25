// Copyright 2015 PingCAP, Inc.
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
//go:build !race
// +build !race

package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"gitee.com/zhoujin826/goInception-plus/domain"
	"gitee.com/zhoujin826/goInception-plus/kv"
	"gitee.com/zhoujin826/goInception-plus/metrics"
	tmysql "gitee.com/zhoujin826/goInception-plus/parser/mysql"
	"gitee.com/zhoujin826/goInception-plus/session"
	"gitee.com/zhoujin826/goInception-plus/sessionctx/variable"
	"gitee.com/zhoujin826/goInception-plus/store/mockstore"
	"gitee.com/zhoujin826/goInception-plus/util"
	"gitee.com/zhoujin826/goInception-plus/util/collate"
	"gitee.com/zhoujin826/goInception-plus/util/logutil"
	"gitee.com/zhoujin826/goInception-plus/util/topsql/reporter"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

type tidbTestSuite struct {
	*tidbTestSuiteBase
}

type tidbTestSerialSuite struct {
	*tidbTestSuiteBase
}

type tidbTestSuiteBase struct {
	*testServerClient
	tidbdrv *TiDBDriver
	server  *Server
	domain  *domain.Domain
	store   kv.Storage
}

func newTiDBTestSuiteBase() *tidbTestSuiteBase {
	return &tidbTestSuiteBase{
		testServerClient: newTestServerClient(),
	}
}

var _ = Suite(&tidbTestSuite{newTiDBTestSuiteBase()})
var _ = SerialSuites(&tidbTestSerialSuite{newTiDBTestSuiteBase()})

func (ts *tidbTestSuite) SetUpSuite(c *C) {
	metrics.RegisterMetrics()
	ts.tidbTestSuiteBase.SetUpSuite(c)
}

func (ts *tidbTestSuite) TearDownSuite(c *C) {
	ts.tidbTestSuiteBase.TearDownSuite(c)
}

func (ts *tidbTestSuiteBase) SetUpSuite(c *C) {
	var err error
	ts.store, err = mockstore.NewMockStore()
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)
	cfg := newTestConfig()
	cfg.Socket = ""
	cfg.Port = ts.port
	cfg.SecondPort = ts.port
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = ts.statusPort
	cfg.Security.SkipGrantTable = false
	cfg.Performance.TCPKeepAlive = true
	err = logutil.InitLogger(cfg.Log.ToLogConfig())
	c.Assert(err, IsNil)

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.port = getPortFromTCPAddr(server.listener.Addr())
	ts.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ts.server = server
	go func() {
		err := ts.server.Run()
		c.Assert(err, IsNil)
	}()
	ts.waitUntilServerOnline()
}

func (ts *tidbTestSuiteBase) TearDownSuite(c *C) {
	if ts.store != nil {
		ts.store.Close()
	}
	if ts.domain != nil {
		ts.domain.Close()
	}
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *tidbTestSuite) TestUint64(c *C) {
	ts.runTestPrepareResultFieldType(c)
}

func (ts *tidbTestSuite) TestStatusAPI(c *C) {
	c.Parallel()
	ts.runTestStatusAPI(c)
}

func (ts *tidbTestSuite) TestStatusPort(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer dom.Close()
	ts.tidbdrv = NewTiDBDriver(store)
	cfg := newTestConfig()
	cfg.Socket = ""
	cfg.Port = 0
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = ts.statusPort
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, NotNil)
	c.Assert(server, IsNil)
}

func newTLSHttpClient(c *C, caFile, certFile, keyFile string) *http.Client {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	c.Assert(err, IsNil)
	caCert, err := os.ReadFile(caFile)
	c.Assert(err, IsNil)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: true,
	}
	tlsConfig.BuildNameToCertificate()
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
}

// TestOnlySocket for server configuration without network interface for mysql clients
func (ts *tidbTestSuite) TestOnlySocket(c *C) {
	osTempDir := os.TempDir()
	tempDir, err := os.MkdirTemp(osTempDir, "tidb-test.*.socket")
	c.Assert(err, IsNil)
	socketFile := tempDir + "/tidbtest.sock" // Unix Socket does not work on Windows, so '/' should be OK
	defer os.RemoveAll(tempDir)
	cli := newTestServerClient()
	cfg := newTestConfig()
	cfg.Socket = socketFile
	cfg.Host = "" // No network interface listening for mysql traffic
	cfg.Status.ReportStatus = false

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	go func() {
		err := server.Run()
		c.Assert(err, IsNil)
	}()
	time.Sleep(time.Millisecond * 100)
	defer server.Close()
	c.Assert(server.listener, IsNil)
	c.Assert(server.socket, NotNil)

	// Test with Socket connection + Setup user1@% for all host access
	defer func() {
		cli.runTests(c, func(config *mysql.Config) {
			config.User = "root"
			config.Net = "unix"
			config.Addr = socketFile
		},
			func(dbt *DBTest) {
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'%'")
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'localhost'")
				dbt.mustQuery("DROP USER IF EXISTS 'user1'@'127.0.0.1'")
			})
	}()
	cli.runTests(c, func(config *mysql.Config) {
		config.User = "root"
		config.Net = "unix"
		config.Addr = socketFile
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@'%'")
			dbt.mustQuery("GRANT SELECT ON test.* TO user1@'%'")
		})
	// Test with Network interface connection with all hosts, should fail since server not configured
	db, err := sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "root"
		config.DBName = "test"
		config.Addr = "127.0.0.1"
	}))
	c.Assert(err, IsNil, Commentf("Connect succeeded when not configured!?!"))
	defer db.Close()
	db, err = sql.Open("mysql", cli.getDSN(func(config *mysql.Config) {
		config.User = "user1"
		config.DBName = "test"
		config.Addr = "127.0.0.1"
	}))
	c.Assert(err, IsNil, Commentf("Connect succeeded when not configured!?!"))
	defer db.Close()
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@127.0.0.1 for loop back network interface access
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			// NOTICE: this is not compatible with MySQL! (MySQL would report user1@localhost also for 127.0.0.1)
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@127.0.0.1")
			dbt.mustQuery("GRANT SELECT,INSERT ON test.* TO user1@'127.0.0.1'")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'%'\nGRANT SELECT ON test.* TO 'user1'@'%'")
		})

	// Setup user1@localhost for socket (and if MySQL compatible; loop back network interface access)
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "root"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "root@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			dbt.mustQuery("CREATE USER user1@localhost")
			dbt.mustQuery("GRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO user1@localhost")
		})
	// Test with unix domain socket file connection with all hosts
	cli.runTests(c, func(config *mysql.Config) {
		config.Net = "unix"
		config.Addr = socketFile
		config.User = "user1"
		config.DBName = "test"
	},
		func(dbt *DBTest) {
			rows := dbt.mustQuery("select user()")
			cli.checkRows(c, rows, "user1@localhost")
			rows = dbt.mustQuery("show grants")
			cli.checkRows(c, rows, "GRANT USAGE ON *.* TO 'user1'@'localhost'\nGRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO 'user1'@'localhost'")
		})

}

// generateCert generates a private key and a certificate in PEM format based on parameters.
// If parentCert and parentCertKey is specified, the new certificate will be signed by the parentCert.
// Otherwise, the new certificate will be self-signed and is a CA.
func generateCert(sn int, commonName string, parentCert *x509.Certificate, parentCertKey *rsa.PrivateKey, outKeyFile string, outCertFile string, opts ...func(c *x509.Certificate)) (*x509.Certificate, *rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 528)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	notBefore := time.Now().Add(-10 * time.Minute).UTC()
	notAfter := notBefore.Add(1 * time.Hour).UTC()

	template := x509.Certificate{
		SerialNumber:          big.NewInt(int64(sn)),
		Subject:               pkix.Name{CommonName: commonName, Names: []pkix.AttributeTypeAndValue{util.MockPkixAttribute(util.CommonName, commonName)}},
		DNSNames:              []string{commonName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	for _, opt := range opts {
		opt(&template)
	}

	var parent *x509.Certificate
	var priv *rsa.PrivateKey

	if parentCert == nil || parentCertKey == nil {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
		parent = &template
		priv = privateKey
	} else {
		parent = parentCert
		priv = parentCertKey
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, parent, &privateKey.PublicKey, priv)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	certOut, err := os.Create(outCertFile)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = certOut.Close()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	keyOut, err := os.OpenFile(outKeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	err = keyOut.Close()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return cert, privateKey, nil
}

func (ts *tidbTestSuite) TestCreateTableFlen(c *C) {
	// issue #4540
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = Execute(context.Background(), qctx, "use test;")
	c.Assert(err, IsNil)

	ctx := context.Background()
	testSQL := "CREATE TABLE `t1` (" +
		"`a` char(36) NOT NULL," +
		"`b` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`d` varchar(50) DEFAULT ''," +
		"`e` char(36) NOT NULL DEFAULT ''," +
		"`f` char(36) NOT NULL DEFAULT ''," +
		"`g` char(1) NOT NULL DEFAULT 'N'," +
		"`h` varchar(100) NOT NULL," +
		"`i` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`j` varchar(10) DEFAULT ''," +
		"`k` varchar(10) DEFAULT ''," +
		"`l` varchar(20) DEFAULT ''," +
		"`m` varchar(20) DEFAULT ''," +
		"`n` varchar(30) DEFAULT ''," +
		"`o` varchar(100) DEFAULT ''," +
		"`p` varchar(50) DEFAULT ''," +
		"`q` varchar(50) DEFAULT ''," +
		"`r` varchar(100) DEFAULT ''," +
		"`s` varchar(20) DEFAULT ''," +
		"`t` varchar(50) DEFAULT ''," +
		"`u` varchar(100) DEFAULT ''," +
		"`v` varchar(50) DEFAULT ''," +
		"`w` varchar(300) NOT NULL," +
		"`x` varchar(250) DEFAULT ''," +
		"`y` decimal(20)," +
		"`z` decimal(20, 4)," +
		"PRIMARY KEY (`a`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	_, err = Execute(ctx, qctx, testSQL)
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "show create table t1")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 2)
	c.Assert(int(cols[0].ColumnLength), Equals, 5*tmysql.MaxBytesOfCharacter)
	c.Assert(int(cols[1].ColumnLength), Equals, len(req.GetRow(0).GetString(1))*tmysql.MaxBytesOfCharacter)

	// for issue#5246
	rs, err = Execute(ctx, qctx, "select y, z from t1")
	c.Assert(err, IsNil)
	cols = rs.Columns()
	c.Assert(len(cols), Equals, 2)
	c.Assert(int(cols[0].ColumnLength), Equals, 21)
	c.Assert(int(cols[1].ColumnLength), Equals, 22)
}

func Execute(ctx context.Context, qc *TiDBContext, sql string) (ResultSet, error) {
	stmts, err := qc.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		panic("wrong input for Execute: " + sql)
	}
	return qc.ExecuteStmt(ctx, stmts[0])
}

func (ts *tidbTestSuite) TestShowTablesFlen(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	c.Assert(err, IsNil)

	testSQL := "create table abcdefghijklmnopqrstuvwxyz (i int)"
	_, err = Execute(ctx, qctx, testSQL)
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "show tables")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(err, IsNil)
	c.Assert(len(cols), Equals, 1)
	c.Assert(int(cols[0].ColumnLength), Equals, 26*tmysql.MaxBytesOfCharacter)
}

func checkColNames(c *C, columns []*ColumnInfo, names ...string) {
	for i, name := range names {
		c.Assert(columns[i].Name, Equals, name)
		c.Assert(columns[i].OrgName, Equals, name)
	}
}

func (ts *tidbTestSuite) TestFieldList(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	_, err = Execute(context.Background(), qctx, "use test;")
	c.Assert(err, IsNil)

	ctx := context.Background()
	testSQL := `create table t (
		c_bit bit(10),
		c_int_d int,
		c_bigint_d bigint,
		c_float_d float,
		c_double_d double,
		c_decimal decimal(6, 3),
		c_datetime datetime(2),
		c_time time(3),
		c_date date,
		c_timestamp timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),
		c_char char(20),
		c_varchar varchar(20),
		c_text_d text,
		c_binary binary(20),
		c_blob_d blob,
		c_set set('a', 'b', 'c'),
		c_enum enum('a', 'b', 'c'),
		c_json JSON,
		c_year year
	)`
	_, err = Execute(ctx, qctx, testSQL)
	c.Assert(err, IsNil)
	colInfos, err := qctx.FieldList("t")
	c.Assert(err, IsNil)
	c.Assert(len(colInfos), Equals, 19)

	checkColNames(c, colInfos, "c_bit", "c_int_d", "c_bigint_d", "c_float_d",
		"c_double_d", "c_decimal", "c_datetime", "c_time", "c_date", "c_timestamp",
		"c_char", "c_varchar", "c_text_d", "c_binary", "c_blob_d", "c_set", "c_enum",
		"c_json", "c_year")

	for _, cols := range colInfos {
		c.Assert(cols.Schema, Equals, "test")
	}

	for _, cols := range colInfos {
		c.Assert(cols.Table, Equals, "t")
	}

	for i, col := range colInfos {
		switch i {
		case 10, 11, 12, 15, 16:
			// c_char char(20), c_varchar varchar(20), c_text_d text,
			// c_set set('a', 'b', 'c'), c_enum enum('a', 'b', 'c')
			c.Assert(col.Charset, Equals, uint16(tmysql.CharsetNameToID(tmysql.DefaultCharset)), Commentf("index %d", i))
			continue
		}

		c.Assert(col.Charset, Equals, uint16(tmysql.CharsetNameToID("binary")), Commentf("index %d", i))
	}

	// c_decimal decimal(6, 3)
	c.Assert(colInfos[5].Decimal, Equals, uint8(3))

	// for issue#10513
	tooLongColumnAsName := "COALESCE(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)"
	columnAsName := tooLongColumnAsName[:tmysql.MaxAliasIdentifierLen]

	rs, err := Execute(ctx, qctx, "select "+tooLongColumnAsName)
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(cols[0].OrgName, Equals, tooLongColumnAsName)
	c.Assert(cols[0].Name, Equals, columnAsName)

	rs, err = Execute(ctx, qctx, "select c_bit as '"+tooLongColumnAsName+"' from t")
	c.Assert(err, IsNil)
	cols = rs.Columns()
	c.Assert(cols[0].OrgName, Equals, "c_bit")
	c.Assert(cols[0].Name, Equals, columnAsName)
}

func (ts *tidbTestSuite) TestNullFlag(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)

	ctx := context.Background()
	{
		// issue #9689
		rs, err := Execute(ctx, qctx, "select 1")
		c.Assert(err, IsNil)
		cols := rs.Columns()
		c.Assert(len(cols), Equals, 1)
		expectFlag := uint16(tmysql.NotNullFlag | tmysql.BinaryFlag)
		c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
	}

	{
		// issue #19025
		rs, err := Execute(ctx, qctx, "select convert('{}', JSON)")
		c.Assert(err, IsNil)
		cols := rs.Columns()
		c.Assert(len(cols), Equals, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
	}

	{
		// issue #18488
		_, err := Execute(ctx, qctx, "use test")
		c.Assert(err, IsNil)
		_, err = Execute(ctx, qctx, "CREATE TABLE `test` (`iD` bigint(20) NOT NULL, `INT_TEST` int(11) DEFAULT NULL);")
		c.Assert(err, IsNil)
		rs, err := Execute(ctx, qctx, `SELECT id + int_test as res FROM test  GROUP BY res ORDER BY res;`)
		c.Assert(err, IsNil)
		cols := rs.Columns()
		c.Assert(len(cols), Equals, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
	}
	{

		rs, err := Execute(ctx, qctx, "select if(1, null, 1) ;")
		c.Assert(err, IsNil)
		cols := rs.Columns()
		c.Assert(len(cols), Equals, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
	}
	{
		rs, err := Execute(ctx, qctx, "select CASE 1 WHEN 2 THEN 1 END ;")
		c.Assert(err, IsNil)
		cols := rs.Columns()
		c.Assert(len(cols), Equals, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
	}
	{
		rs, err := Execute(ctx, qctx, "select NULL;")
		c.Assert(err, IsNil)
		cols := rs.Columns()
		c.Assert(len(cols), Equals, 1)
		expectFlag := uint16(tmysql.BinaryFlag)
		c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
	}
}

func (ts *tidbTestSuite) TestNO_DEFAULT_VALUEFlag(c *C) {
	// issue #21465
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)

	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "drop table if exists t")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t(c1 int key, c2 int);")
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "select c1 from t;")
	c.Assert(err, IsNil)
	cols := rs.Columns()
	c.Assert(len(cols), Equals, 1)
	expectFlag := uint16(tmysql.NotNullFlag | tmysql.PriKeyFlag | tmysql.NoDefaultValueFlag)
	c.Assert(dumpFlag(cols[0].Type, cols[0].Flag), Equals, expectFlag)
}

func (ts *tidbTestSerialSuite) TestDefaultCharacterAndCollation(c *C) {
	// issue #21194
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	// 255 is the collation id of mysql client 8 default collation_connection
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(255), "test", nil)
	c.Assert(err, IsNil)
	testCase := []struct {
		variable string
		except   string
	}{
		{"collation_connection", "utf8mb4_bin"},
		{"character_set_connection", "utf8mb4"},
		{"character_set_client", "utf8mb4"},
	}

	for _, t := range testCase {
		sVars, b := qctx.GetSessionVars().GetSystemVar(t.variable)
		c.Assert(b, IsTrue)
		c.Assert(sVars, Equals, t.except)
	}
}

func (ts *tidbTestSuite) TestPessimisticInsertSelectForUpdate(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	defer qctx.Close()
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "drop table if exists t1, t2")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t1 (id int)")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t2 (id int)")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "insert into t1 select 1")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "begin pessimistic")
	c.Assert(err, IsNil)
	rs, err := Execute(ctx, qctx, "INSERT INTO t2 (id) select id from t1 where id = 1 for update")
	c.Assert(err, IsNil)
	c.Assert(rs, IsNil) // should be no delay
}

func (ts *tidbTestSerialSuite) TestPrepareCount(c *C) {
	qctx, err := ts.tidbdrv.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "test", nil)
	c.Assert(err, IsNil)
	prepareCnt := atomic.LoadInt64(&variable.PreparedStmtCount)
	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test;")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "drop table if exists t1")
	c.Assert(err, IsNil)
	_, err = Execute(ctx, qctx, "create table t1 (id int)")
	c.Assert(err, IsNil)
	stmt, _, _, err := qctx.Prepare("insert into t1 values (?)", "")
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadInt64(&variable.PreparedStmtCount), Equals, prepareCnt+1)
	c.Assert(err, IsNil)
	err = qctx.GetStatement(stmt.ID()).Close()
	c.Assert(err, IsNil)
	c.Assert(atomic.LoadInt64(&variable.PreparedStmtCount), Equals, prepareCnt)
}

type collectorWrapper struct {
	reporter.TopSQLReporter
}
