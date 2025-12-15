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

package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"gitee.com/zhoujin826/goInception-plus/bindinfo"
	"gitee.com/zhoujin826/goInception-plus/config"
	"gitee.com/zhoujin826/goInception-plus/ddl"
	"gitee.com/zhoujin826/goInception-plus/domain"
	"gitee.com/zhoujin826/goInception-plus/executor"
	"gitee.com/zhoujin826/goInception-plus/kv"
	"gitee.com/zhoujin826/goInception-plus/metrics"
	"gitee.com/zhoujin826/goInception-plus/parser/mysql"
	"gitee.com/zhoujin826/goInception-plus/parser/terror"
	parsertypes "gitee.com/zhoujin826/goInception-plus/parser/types"
	plannercore "gitee.com/zhoujin826/goInception-plus/planner/core"
	"gitee.com/zhoujin826/goInception-plus/plugin"
	"gitee.com/zhoujin826/goInception-plus/privilege/privileges"
	"gitee.com/zhoujin826/goInception-plus/server"
	"gitee.com/zhoujin826/goInception-plus/session"
	"gitee.com/zhoujin826/goInception-plus/sessionctx/binloginfo"
	"gitee.com/zhoujin826/goInception-plus/sessionctx/variable"
	"gitee.com/zhoujin826/goInception-plus/statistics"
	kvstore "gitee.com/zhoujin826/goInception-plus/store"
	"gitee.com/zhoujin826/goInception-plus/store/driver"
	"gitee.com/zhoujin826/goInception-plus/store/mockstore"
	"gitee.com/zhoujin826/goInception-plus/util"
	"gitee.com/zhoujin826/goInception-plus/util/deadlockhistory"
	"gitee.com/zhoujin826/goInception-plus/util/disk"
	"gitee.com/zhoujin826/goInception-plus/util/domainutil"
	"gitee.com/zhoujin826/goInception-plus/util/kvcache"
	"gitee.com/zhoujin826/goInception-plus/util/logutil"
	"gitee.com/zhoujin826/goInception-plus/util/memory"
	"gitee.com/zhoujin826/goInception-plus/util/printer"
	"gitee.com/zhoujin826/goInception-plus/util/profile"
	"gitee.com/zhoujin826/goInception-plus/util/sem"
	"gitee.com/zhoujin826/goInception-plus/util/signal"
	"gitee.com/zhoujin826/goInception-plus/util/sys/linux"
	storageSys "gitee.com/zhoujin826/goInception-plus/util/sys/storage"
	"gitee.com/zhoujin826/goInception-plus/util/systimemon"
	"gitee.com/zhoujin826/goInception-plus/util/topsql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	plog "github.com/sirupsen/logrus"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

// Flag Names
const (
	nmVersion                = "V"
	nmConfig                 = "config"
	nmConfigCheck            = "config-check"
	nmConfigStrict           = "config-strict"
	nmStore                  = "store"
	nmStorePath              = "path"
	nmHost                   = "host"
	nmAdvertiseAddress       = "advertise-address"
	nmPort                   = "P"
	nmCors                   = "cors"
	nmSocket                 = "socket"
	nmEnableBinlog           = "enable-binlog"
	nmRunDDL                 = "run-ddl"
	nmLogLevel               = "L"
	nmLogFile                = "log-file"
	nmLogSlowQuery           = "log-slow-query"
	nmReportStatus           = "report-status"
	nmStatusHost             = "status-host"
	nmStatusPort             = "status"
	nmMetricsAddr            = "metrics-addr"
	nmMetricsInterval        = "metrics-interval"
	nmDdlLease               = "lease"
	nmTokenLimit             = "token-limit"
	nmPluginDir              = "plugin-dir"
	nmPluginLoad             = "plugin-load"
	nmRepairMode             = "repair-mode"
	nmRepairList             = "repair-list"
	nmRequireSecureTransport = "require-secure-transport"

	nmProxyProtocolNetworks      = "proxy-protocol-networks"
	nmProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
	nmAffinityCPU                = "affinity-cpus"

	nmInitializeSecure   = "initialize-secure"
	nmInitializeInsecure = "initialize-insecure"
)

var (
	version      = flagBoolean(nmVersion, false, "print version information and exit")
	configPath   = flag.String(nmConfig, "", "config file path")
	configCheck  = flagBoolean(nmConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(nmConfigStrict, false, "enforce config file validity")

	// Base
	store            = flag.String(nmStore, "unistore", "registered store name, [tikv, mocktikv, unistore]")
	storePath        = flag.String(nmStorePath, "/tmp/tidb", "tidb storage path")
	host             = flag.String(nmHost, "0.0.0.0", "tidb server host")
	advertiseAddress = flag.String(nmAdvertiseAddress, "", "tidb server advertise IP")
	port             = flag.String(nmPort, "4000", "tidb server port")
	cors             = flag.String(nmCors, "", "tidb server allow cors origin")
	socket           = flag.String(nmSocket, "/tmp/tidb-{Port}.sock", "The socket file to use for connection.")
	enableBinlog     = flagBoolean(nmEnableBinlog, false, "enable generate binlog")
	runDDL           = flagBoolean(nmRunDDL, true, "run ddl worker on this tidb-server")
	ddlLease         = flag.String(nmDdlLease, "45s", "schema lease duration, very dangerous to change only if you know what you do")
	tokenLimit       = flag.Int(nmTokenLimit, 1000, "the limit of concurrent executed sessions")
	pluginDir        = flag.String(nmPluginDir, "/data/deploy/plugin", "the folder that hold plugin")
	pluginLoad       = flag.String(nmPluginLoad, "", "wait load plugin name(separated by comma)")
	affinityCPU      = flag.String(nmAffinityCPU, "", "affinity cpu (cpu-no. separated by comma, e.g. 1,2,3)")
	repairMode       = flagBoolean(nmRepairMode, false, "enable admin repair mode")
	repairList       = flag.String(nmRepairList, "", "admin repair table list")
	requireTLS       = flag.Bool(nmRequireSecureTransport, false, "require client use secure transport")

	// Log
	logLevel     = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile      = flag.String(nmLogFile, "", "log file path")
	logSlowQuery = flag.String(nmLogSlowQuery, "", "slow query file path")

	// Status
	reportStatus    = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusHost      = flag.String(nmStatusHost, "0.0.0.0", "tidb server status host")
	statusPort      = flag.String(nmStatusPort, "10080", "tidb server status port")
	metricsAddr     = flag.String(nmMetricsAddr, "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Uint(nmMetricsInterval, 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")

	// PROXY Protocol
	proxyProtocolNetworks      = flag.String(nmProxyProtocolNetworks, "", "proxy protocol networks allowed IP or *, empty mean disable proxy protocol support")
	proxyProtocolHeaderTimeout = flag.Uint(nmProxyProtocolHeaderTimeout, 5, "proxy protocol header read timeout, unit is second.")

	// Security
	initializeSecure   = flagBoolean(nmInitializeSecure, false, "bootstrap tidb-server in secure mode")
	initializeInsecure = flagBoolean(nmInitializeInsecure, true, "bootstrap tidb-server in insecure mode")
)

var (
	cfg      *config.Config
	storage  kv.Storage
	dom      *domain.Domain
	svr      *server.Server
	graceful bool
)

func main() {
	help := flag.Bool("help", false, "show the usage")
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(0)
	}
	if *version {
		fmt.Println(printer.GetTiDBInfo())
		os.Exit(0)
	}
	registerStores()
	loadConfig()
	//registerMetrics()
	config.InitializeConfig(*configPath, *configCheck, *configStrict, overrideConfig)
	/*if config.GetGlobalConfig().OOMUseTmpStorage {
		config.GetGlobalConfig().UpdateTempStoragePath()
		err := disk.InitializeTempDir()
		terror.MustNil(err)
		checkTempStorageQuota()
	}*/
	setGlobalVars()
	//setCPUAffinity()
	setupLog()
	//setHeapProfileTracker()
	//setupTracing() // Should before createServer and after setup config.
	printInfo()
	setupBinlogClient()
	//setupMetrics()

	storage, dom := createStoreAndDomain()
	svr := createServer(storage, dom)

	// Register error API is not thread-safe, the caller MUST NOT register errors after initialization.
	// To prevent misuse, set a flag to indicate that register new error will panic immediately.
	// For regression of issue like https://gitee.com/zhoujin826/goInception-plus/issues/28190
	terror.RegisterFinish()

	exited := make(chan struct{})
	signal.SetupSignalHandler(func(graceful bool) {
		svr.Close()
		cleanup(svr, storage, dom, graceful)
		close(exited)
	})
	// 在启动完成后关闭DDL线程(goInception用不到该线程)
	ddl := dom.DDL()
	if ddl != nil {
		terror.Log(errors.Trace(ddl.Stop()))
	}
	if config.GetGlobalConfig().Inc.EnableBlobType ||
		config.GetGlobalConfig().Inc.EnableJsonType ||
		config.GetGlobalConfig().Inc.EnableTimeStampType ||
		config.GetGlobalConfig().Inc.EnableEnumSetBit {
		fmt.Println("################################################")
		fmt.Println("Warning: The following parameters will be deprecated and replaced with disable_types:")
		fmt.Println("\tenable_blob_type")
		fmt.Println("\tenable_json_type")
		fmt.Println("\tenable_enum_set_bit")
		fmt.Println("\tenable_timestamp_type")
		fmt.Println("https://github.com/hanchuanchuan/goInception/pull/418")
		fmt.Println("################################################")
	}
	//topsql.SetupTopSQL()
	terror.MustNil(svr.Run())
	<-exited
	//syncLog()
}

func exit() {
	syncLog()
	os.Exit(0)
}

func syncLog() {
	if err := log.Sync(); err != nil {
		// Don't complain about /dev/stdout as Fsync will return EINVAL.
		if pathErr, ok := err.(*fs.PathError); ok {
			if pathErr.Path == "/dev/stdout" {
				os.Exit(0)
			}
		}
		fmt.Fprintln(os.Stderr, "sync log err:", err)
		os.Exit(1)
	}
}

func checkTempStorageQuota() {
	// check capacity and the quota when OOMUseTmpStorage is enabled
	c := config.GetGlobalConfig()
	if c.TempStorageQuota < 0 {
		// means unlimited, do nothing
	} else {
		capacityByte, err := storageSys.GetTargetDirectoryCapacity(c.TempStoragePath)
		if err != nil {
			log.Fatal(err.Error())
		} else if capacityByte < uint64(c.TempStorageQuota) {
			log.Fatal(fmt.Sprintf("value of [tmp-storage-quota](%d byte) exceeds the capacity(%d byte) of the [%s] directory", c.TempStorageQuota, capacityByte, c.TempStoragePath))
		}
	}
}

func setCPUAffinity() {
	if affinityCPU == nil || len(*affinityCPU) == 0 {
		return
	}
	var cpu []int
	for _, af := range strings.Split(*affinityCPU, ",") {
		af = strings.TrimSpace(af)
		if len(af) > 0 {
			c, err := strconv.Atoi(af)
			if err != nil {
				fmt.Fprintf(os.Stderr, "wrong affinity cpu config: %s", *affinityCPU)
				exit()
			}
			cpu = append(cpu, c)
		}
	}
	err := linux.SetAffinity(cpu)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set cpu affinity failure: %v", err)
		exit()
	}
	runtime.GOMAXPROCS(len(cpu))
	metrics.MaxProcs.Set(float64(runtime.GOMAXPROCS(0)))
}

func setHeapProfileTracker() {
	c := config.GetGlobalConfig()
	d := parseDuration(c.Performance.MemProfileInterval)
	go profile.HeapProfileForGlobalMemTracker(d)
}

func registerStores() {
	err := kvstore.Register("tikv", driver.TiKVDriver{})
	terror.MustNil(err)
	err = kvstore.Register("mocktikv", mockstore.MockTiKVDriver{})
	terror.MustNil(err)
	err = kvstore.Register("unistore", mockstore.EmbedUnistoreDriver{})
	terror.MustNil(err)
}

func registerMetrics() {
	metrics.RegisterMetrics()
}

func createStoreAndDomain() (kv.Storage, *domain.Domain) {
	cfg := config.GetGlobalConfig()
	fullPath := fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	var err error
	storage, err := kvstore.New(fullPath)
	terror.MustNil(err)
	// Bootstrap a session to load information schema.
	dom, err := session.BootstrapSession(storage)
	terror.MustNil(err)
	return storage, dom
}

func setupBinlogClient() {
	cfg := config.GetGlobalConfig()
	if !cfg.Binlog.Enable {
		return
	}

	if cfg.Binlog.IgnoreError {
		binloginfo.SetIgnoreError(true)
	}

	var (
		client *pumpcli.PumpsClient
		err    error
	)

	securityOption := pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}

	if len(cfg.Binlog.BinlogSocket) == 0 {
		client, err = pumpcli.NewPumpsClient(cfg.Path, cfg.Binlog.Strategy, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
	} else {
		client, err = pumpcli.NewLocalPumpsClient(cfg.Path, cfg.Binlog.BinlogSocket, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
	}

	terror.MustNil(err)

	err = logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	binloginfo.SetPumpsClient(client)
	log.Info("tidb-server", zap.Bool("create pumps client success, ignore binlog error", cfg.Binlog.IgnoreError))
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// pushMetric pushes metrics in background.
func pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Info("disable Prometheus push client")
		return
	}
	log.Info("start prometheus push client", zap.String("server addr", addr), zap.String("interval", interval.String()))
	go prometheusPushClient(addr, interval)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: TiDB do not have uniq name, so we use host+port to compose a name.
	job := "tidb"
	pusher := push.New(addr, job)
	pusher = pusher.Gatherer(prometheus.DefaultGatherer)
	pusher = pusher.Grouping("instance", instanceName())
	for {
		err := pusher.Push()
		if err != nil {
			log.Error("could not push metrics to prometheus pushgateway", zap.String("err", err.Error()))
		}
		time.Sleep(interval)
	}
}

func instanceName() string {
	cfg := config.GetGlobalConfig()
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%d", hostname, cfg.Port)
}

// parseDuration parses lease argument string.
func parseDuration(lease string) time.Duration {
	dur, err := time.ParseDuration(lease)
	if err != nil {
		dur, err = time.ParseDuration(lease + "s")
	}
	if err != nil || dur < 0 {
		log.Fatal("invalid lease duration", zap.String("lease", lease))
	}
	return dur
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

func loadConfig() {
	cfg = config.GetGlobalConfig()
	if *configPath != "" {
		err := cfg.Load(*configPath)
		terror.MustNil(err)
	} else {
		fmt.Println("################################################")
		fmt.Println("#     Warning: Unspecified config file!        #")
		fmt.Println("################################################")
	}
}

// overrideConfig considers command arguments and overrides some config items in the Config.
func overrideConfig(cfg *config.Config) {
	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})

	// Base
	if actualFlags[nmHost] {
		cfg.Host = *host
	}
	if actualFlags[nmAdvertiseAddress] {
		var err error
		if len(strings.Split(*advertiseAddress, " ")) > 1 {
			err = errors.Errorf("Only support one advertise-address")
		}
		terror.MustNil(err)
		cfg.AdvertiseAddress = *advertiseAddress
	}
	if len(cfg.AdvertiseAddress) == 0 && cfg.Host == "0.0.0.0" {
		cfg.AdvertiseAddress = util.GetLocalIP()
	}
	if len(cfg.AdvertiseAddress) == 0 {
		cfg.AdvertiseAddress = cfg.Host
	}
	var err error
	if actualFlags[nmPort] {
		var p int
		p, err = strconv.Atoi(*port)
		terror.MustNil(err)
		cfg.Port = uint(p)
	}
	if actualFlags[nmCors] {
		fmt.Println(cors)
		cfg.Cors = *cors
	}
	if actualFlags[nmStore] {
		cfg.Store = *store
	}
	if actualFlags[nmStorePath] {
		cfg.Path = *storePath
	}
	if actualFlags[nmSocket] {
		cfg.Socket = *socket
	}
	if actualFlags[nmEnableBinlog] {
		cfg.Binlog.Enable = *enableBinlog
	}
	if actualFlags[nmRunDDL] {
		cfg.RunDDL = *runDDL
	}
	if actualFlags[nmDdlLease] {
		cfg.Lease = *ddlLease
	}
	if actualFlags[nmTokenLimit] {
		cfg.TokenLimit = uint(*tokenLimit)
	}
	if actualFlags[nmPluginLoad] {
		cfg.Plugin.Load = *pluginLoad
	}
	if actualFlags[nmPluginDir] {
		cfg.Plugin.Dir = *pluginDir
	}
	if actualFlags[nmRequireSecureTransport] {
		cfg.Security.RequireSecureTransport = *requireTLS
	}
	if actualFlags[nmRepairMode] {
		cfg.RepairMode = *repairMode
	}
	if actualFlags[nmRepairList] {
		if cfg.RepairMode {
			cfg.RepairTableList = stringToList(*repairList)
		}
	}

	// Log
	if actualFlags[nmLogLevel] {
		cfg.Log.Level = *logLevel
	}
	if actualFlags[nmLogFile] {
		cfg.Log.File.Filename = *logFile
	}
	if actualFlags[nmLogSlowQuery] {
		cfg.Log.SlowQueryFile = *logSlowQuery
	}

	// Status
	if actualFlags[nmReportStatus] {
		cfg.Status.ReportStatus = *reportStatus
	}
	if actualFlags[nmStatusHost] {
		cfg.Status.StatusHost = *statusHost
	}
	if actualFlags[nmStatusPort] {
		var p int
		p, err = strconv.Atoi(*statusPort)
		terror.MustNil(err)
		cfg.Status.StatusPort = uint(p)
	}
	if actualFlags[nmMetricsAddr] {
		cfg.Status.MetricsAddr = *metricsAddr
	}
	if actualFlags[nmMetricsInterval] {
		cfg.Status.MetricsInterval = *metricsInterval
	}

	// PROXY Protocol
	if actualFlags[nmProxyProtocolNetworks] {
		cfg.ProxyProtocol.Networks = *proxyProtocolNetworks
	}
	if actualFlags[nmProxyProtocolHeaderTimeout] {
		cfg.ProxyProtocol.HeaderTimeout = *proxyProtocolHeaderTimeout
	}

	// Sanity check: can't specify both options
	if actualFlags[nmInitializeSecure] && actualFlags[nmInitializeInsecure] {
		err = fmt.Errorf("the options --initialize-insecure and --initialize-secure are mutually exclusive")
		terror.MustNil(err)
	}
	// The option --initialize-secure=true ensures that a secure bootstrap is used.
	if actualFlags[nmInitializeSecure] {
		cfg.Security.SecureBootstrap = *initializeSecure
	}
	// The option --initialize-insecure=true/false was used.
	// Store the inverted value of this to the secure bootstrap cfg item
	if actualFlags[nmInitializeInsecure] {
		cfg.Security.SecureBootstrap = !*initializeInsecure
	}
	// Secure bootstrap initializes with Socket authentication
	// which is not supported on windows. Only the insecure bootstrap
	// method is supported.
	if runtime.GOOS == "windows" && cfg.Security.SecureBootstrap {
		err = fmt.Errorf("the option --initialize-secure is not supported on Windows")
		terror.MustNil(err)
	}

	// cofiguration support environment variables
	if backupHost := os.Getenv("BACKUP_HOST"); backupHost != "" {
		cfg.Inc.BackupHost = backupHost
	}
	if backupPort := os.Getenv("BACKUP_PORT"); backupPort != "" {
		portUint, err := strconv.ParseUint(backupPort, 10, 16)
		if err != nil {
			plog.Errorf("backup port should be between 0 and 65535.")
			os.Exit(-1)
		}
		cfg.Inc.BackupPort = uint(portUint)
	}
	if backupUser := os.Getenv("BACKUP_USER"); backupUser != "" {
		cfg.Inc.BackupUser = backupUser
	}
	if backupPassword := os.Getenv("BACKUP_PASSWORD"); backupPassword != "" {
		cfg.Inc.BackupPassword = backupPassword
	}
}

func setGlobalVars() {
	cfg := config.GetGlobalConfig()

	// Disable automaxprocs log
	nopLog := func(string, ...interface{}) {}
	_, err := maxprocs.Set(maxprocs.Logger(nopLog))
	terror.MustNil(err)
	// We should respect to user's settings in config file.
	// The default value of MaxProcs is 0, runtime.GOMAXPROCS(0) is no-op.
	runtime.GOMAXPROCS(int(cfg.Performance.MaxProcs))
	metrics.MaxProcs.Set(float64(runtime.GOMAXPROCS(0)))

	util.SetGOGC(cfg.Performance.GOGC)

	ddlLeaseDuration := parseDuration(cfg.Lease)
	session.SetSchemaLease(ddlLeaseDuration)
	statsLeaseDuration := parseDuration(cfg.Performance.StatsLease)
	session.SetStatsLease(statsLeaseDuration)
	indexUsageSyncLeaseDuration := parseDuration(cfg.Performance.IndexUsageSyncLease)
	session.SetIndexUsageSyncLease(indexUsageSyncLeaseDuration)
	planReplayerGCLease := parseDuration(cfg.Performance.PlanReplayerGCLease)
	session.SetPlanReplayerGCLease(planReplayerGCLease)
	bindinfo.Lease = parseDuration(cfg.Performance.BindInfoLease)
	domain.RunAutoAnalyze = cfg.Performance.RunAutoAnalyze
	statistics.FeedbackProbability.Store(cfg.Performance.FeedbackProbability)
	statistics.MaxQueryFeedbackCount.Store(int64(cfg.Performance.QueryFeedbackLimit))
	statistics.RatioOfPseudoEstimate.Store(cfg.Performance.PseudoEstimateRatio)
	ddl.RunWorker = cfg.RunDDL
	if cfg.SplitTable {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	}
	plannercore.AllowCartesianProduct.Store(cfg.Performance.CrossJoin)
	// 权限参数冗余设置,开启任一鉴权即可,默认跳过鉴权
	skip := cfg.Security.SkipGrantTable && cfg.Inc.SkipGrantTable
	cfg.Security.SkipGrantTable = skip
	cfg.Inc.SkipGrantTable = skip
	privileges.SkipWithGrant = skip
	privileges.SkipWithGrant = cfg.Security.SkipGrantTable
	kv.TxnTotalSizeLimit = cfg.Performance.TxnTotalSizeLimit
	if cfg.Performance.TxnEntrySizeLimit > 120*1024*1024 {
		log.Fatal("cannot set txn entry size limit larger than 120M")
	}
	kv.TxnEntrySizeLimit = cfg.Performance.TxnEntrySizeLimit

	priority := mysql.Str2Priority(cfg.Performance.ForcePriority)
	variable.ForcePriority = int32(priority)

	if len(cfg.ServerVersion) > 0 {
		mysql.ServerVersion = cfg.ServerVersion
		variable.SetSysVar(variable.Version, cfg.ServerVersion)
	}

	variable.SetSysVar(variable.TiDBForcePriority, mysql.Priority2Str[priority])
	variable.SetSysVar(variable.TiDBOptDistinctAggPushDown, variable.BoolToOnOff(cfg.Performance.DistinctAggPushDown))
	variable.SetSysVar(variable.TiDBMemQuotaQuery, strconv.FormatInt(cfg.MemQuotaQuery, 10))
	variable.SetSysVar(variable.LowerCaseTableNames, strconv.Itoa(cfg.LowerCaseTableNames))
	variable.SetSysVar(variable.LogBin, variable.BoolToOnOff(cfg.Binlog.Enable))
	variable.SetSysVar(variable.Port, fmt.Sprintf("%d", cfg.Port))
	cfg.Socket = strings.Replace(cfg.Socket, "{Port}", fmt.Sprintf("%d", cfg.Port), 1)
	variable.SetSysVar(variable.Socket, cfg.Socket)
	variable.SetSysVar(variable.DataDir, cfg.Path)
	variable.SetSysVar(variable.TiDBSlowQueryFile, cfg.Log.SlowQueryFile)
	variable.SetSysVar(variable.TiDBIsolationReadEngines, strings.Join(cfg.IsolationRead.Engines, ","))
	variable.SetSysVar(variable.TiDBEnforceMPPExecution, variable.BoolToOnOff(config.GetGlobalConfig().Performance.EnforceMPP))
	variable.MemoryUsageAlarmRatio.Store(cfg.Performance.MemoryUsageAlarmRatio)
	if hostname, err := os.Hostname(); err != nil {
		variable.SetSysVar(variable.Hostname, hostname)
	}
	variable.GlobalLogMaxDays.Store(int32(config.GetGlobalConfig().Log.File.MaxDays))

	if cfg.Security.EnableSEM {
		sem.Enable()
	}

	// For CI environment we default enable prepare-plan-cache.
	plannercore.SetPreparedPlanCache(config.CheckTableBeforeDrop || cfg.PreparedPlanCache.Enabled)
	if plannercore.PreparedPlanCacheEnabled() {
		plannercore.PreparedPlanCacheCapacity = cfg.PreparedPlanCache.Capacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = cfg.PreparedPlanCache.MemoryGuardRatio
		if plannercore.PreparedPlanCacheMemoryGuardRatio < 0.0 || plannercore.PreparedPlanCacheMemoryGuardRatio > 1.0 {
			plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		}
		plannercore.PreparedPlanCacheMaxMemory.Store(cfg.Performance.ServerMemoryQuota)
		total, err := memory.MemTotal()
		terror.MustNil(err)
		if plannercore.PreparedPlanCacheMaxMemory.Load() > total || plannercore.PreparedPlanCacheMaxMemory.Load() <= 0 {
			plannercore.PreparedPlanCacheMaxMemory.Store(total)
		}
	}

	atomic.StoreUint64(&transaction.CommitMaxBackoff, uint64(parseDuration(cfg.TiKVClient.CommitTimeout).Seconds()*1000))
	tikv.SetRegionCacheTTLSec(int64(cfg.TiKVClient.RegionCacheTTL))
	domainutil.RepairInfo.SetRepairMode(cfg.RepairMode)
	domainutil.RepairInfo.SetRepairTableList(cfg.RepairTableList)
	executor.GlobalDiskUsageTracker.SetBytesLimit(cfg.TempStorageQuota)
	if cfg.Performance.ServerMemoryQuota < 1 {
		// If MaxMemory equals 0, it means unlimited
		executor.GlobalMemoryUsageTracker.SetBytesLimit(-1)
	} else {
		executor.GlobalMemoryUsageTracker.SetBytesLimit(int64(cfg.Performance.ServerMemoryQuota))
	}
	kvcache.GlobalLRUMemUsageTracker.AttachToGlobalTracker(executor.GlobalMemoryUsageTracker)

	t, err := time.ParseDuration(cfg.TiKVClient.StoreLivenessTimeout)
	if err != nil || t < 0 {
		logutil.BgLogger().Fatal("invalid duration value for store-liveness-timeout",
			zap.String("currentValue", cfg.TiKVClient.StoreLivenessTimeout))
	}
	tikv.SetStoreLivenessTimeout(t)
	parsertypes.TiDBStrictIntegerDisplayWidth = cfg.DeprecateIntegerDisplayWidth
	deadlockhistory.GlobalDeadlockHistory.Resize(cfg.PessimisticTxn.DeadlockHistoryCapacity)
}

func setupLog() {
	cfg := config.GetGlobalConfig()
	err := logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	// trigger internal http(s) client init.
	util.InternalHTTPClient()
}

func printInfo() {
	// Make sure the TiDB info is always printed.
	level := log.GetLevel()
	log.SetLevel(zap.InfoLevel)
	printer.PrintTiDBInfo()
	log.SetLevel(level)
}

func createServer(storage kv.Storage, dom *domain.Domain) *server.Server {
	cfg := config.GetGlobalConfig()
	driver := server.NewTiDBDriver(storage)
	svr, err := server.NewServer(cfg, driver)
	// Both domain and storage have started, so we have to clean them before exiting.
	if err != nil {
		closeDomainAndStorage(storage, dom)
		log.Fatal("failed to create the server", zap.Error(err), zap.Stack("stack"))
	}
	svr.SetDomain(dom)
	svr.InitGlobalConnID(dom.ServerID)
	go dom.ExpensiveQueryHandle().SetSessionManager(svr).Run()
	dom.InfoSyncer().SetSessionManager(svr)
	return svr
}

func setupMetrics() {
	cfg := config.GetGlobalConfig()
	// Enable the mutex profile, 1/10 of mutex blocking event sampling.
	runtime.SetMutexProfileFraction(10)
	systimeErrHandler := func() {
		metrics.TimeJumpBackCounter.Inc()
	}
	callBackCount := 0
	successCallBack := func() {
		callBackCount++
		// It is callback by monitor per second, we increase metrics.KeepAliveCounter per 5s.
		if callBackCount >= 5 {
			callBackCount = 0
			metrics.KeepAliveCounter.Inc()
		}
	}
	go systimemon.StartMonitor(time.Now, systimeErrHandler, successCallBack)

	pushMetric(cfg.Status.MetricsAddr, time.Duration(cfg.Status.MetricsInterval)*time.Second)
}

func setupTracing() {
	cfg := config.GetGlobalConfig()
	tracingCfg := cfg.OpenTracing.ToTracingConfig()
	tracingCfg.ServiceName = "TiDB"
	tracer, _, err := tracingCfg.NewTracer()
	if err != nil {
		log.Fatal("setup jaeger tracer failed", zap.String("error message", err.Error()))
	}
	opentracing.SetGlobalTracer(tracer)
}

func closeDomainAndStorage(storage kv.Storage, dom *domain.Domain) {
	tikv.StoreShuttingDown(1)
	dom.Close()
	err := storage.Close()
	terror.Log(errors.Trace(err))
}

func cleanup(svr *server.Server, storage kv.Storage, dom *domain.Domain, graceful bool) {
	if graceful {
		done := make(chan struct{})
		svr.GracefulDown(context.Background(), done)
	} else {
		svr.TryGracefulDown()
	}
	plugin.Shutdown(context.Background())
	closeDomainAndStorage(storage, dom)
	disk.CleanUp()
	topsql.Close()
}

func stringToList(repairString string) []string {
	if len(repairString) <= 0 {
		return []string{}
	}
	if repairString[0] == '[' && repairString[len(repairString)-1] == ']' {
		repairString = repairString[1 : len(repairString)-1]
	}
	return strings.FieldsFunc(repairString, func(r rune) bool {
		return r == ',' || r == ' ' || r == '"'
	})
}
