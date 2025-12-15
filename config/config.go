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

package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"gitee.com/zhoujin826/goInception-plus/parser/terror"
	"gitee.com/zhoujin826/goInception-plus/util/logutil"
	"gitee.com/zhoujin826/goInception-plus/util/versioninfo"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tracing "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
)

// Config number limitations
const (
	MaxLogFileSize = 4096 // MB
	// DefTxnEntrySizeLimit is the default value of TxnEntrySizeLimit.
	DefTxnEntrySizeLimit = 6 * 1024 * 1024
	// DefTxnTotalSizeLimit is the default value of TxnTxnTotalSizeLimit.
	DefTxnTotalSizeLimit = 100 * 1024 * 1024
	// DefMaxIndexLength is the maximum index length(in bytes). This value is consistent with MySQL.
	DefMaxIndexLength = 3072
	// DefMaxOfMaxIndexLength is the maximum index length(in bytes) for TiDB v3.0.7 and previous version.
	DefMaxOfMaxIndexLength = 3072 * 4
	// DefIndexLimit is the limitation of index on a single table. This value is consistent with MySQL.
	DefIndexLimit = 64
	// DefMaxOfIndexLimit is the maximum limitation of index on a single table for TiDB.
	DefMaxOfIndexLimit = 64 * 8
	// DefPort is the default port of TiDB
	DefPort = 4000
	// DefPort is the default port of TiDBForPostgreSQL
	DefSecondPort = 5000
	// DefStatusPort is the default status port of TiDB
	DefStatusPort = 10080
	// DefHost is the default host of TiDB
	DefHost = "0.0.0.0"
	// DefStatusHost is the default status host of TiDB
	DefStatusHost = "0.0.0.0"
	// DefTableColumnCountLimit is limit of the number of columns in a table
	DefTableColumnCountLimit = 1017
	// DefMaxOfTableColumnCountLimit is maximum limitation of the number of columns in a table
	DefMaxOfTableColumnCountLimit = 4096
)

// Valid config maps
var (
	ValidStorage = map[string]bool{
		"mocktikv": true,
		"tikv":     true,
		"unistore": true,
	}
	// CheckTableBeforeDrop enable to execute `admin check table` before `drop table`.
	CheckTableBeforeDrop = false
	// checkBeforeDropLDFlag is a go build flag.
	checkBeforeDropLDFlag = "None"
	// tempStorageDirName is the default temporary storage dir name by base64 encoding a string `port/statusPort`
	tempStorageDirName = encodeDefTempStorageDir(os.TempDir(), DefHost, DefStatusHost, DefPort, DefStatusPort)
)

// Config contains configuration options.
type Config struct {
	Host             string `toml:"host" json:"host"`
	AdvertiseAddress string `toml:"advertise-address" json:"advertise-address"`
	Port             uint   `toml:"port" json:"port"`
	SecondPort       uint   `toml:"second-port" json:"second-port"`
	Cors             string `toml:"cors" json:"cors"`
	Store            string `toml:"store" json:"store"`
	Path             string `toml:"path" json:"path"`
	Socket           string `toml:"socket" json:"socket"`
	Lease            string `toml:"lease" json:"lease"`
	RunDDL           bool   `toml:"run-ddl" json:"run-ddl"`
	SplitTable       bool   `toml:"split-table" json:"split-table"`
	TokenLimit       uint   `toml:"token-limit" json:"token-limit"`
	OOMUseTmpStorage bool   `toml:"oom-use-tmp-storage" json:"oom-use-tmp-storage"`
	TempStoragePath  string `toml:"tmp-storage-path" json:"tmp-storage-path"`
	OOMAction        string `toml:"oom-action" json:"oom-action"`
	MemQuotaQuery    int64  `toml:"mem-quota-query" json:"mem-quota-query"`
	// TempStorageQuota describe the temporary storage Quota during query exector when OOMUseTmpStorage is enabled
	// If the quota exceed the capacity of the TempStoragePath, the tidb-server would exit with fatal error
	TempStorageQuota int64 `toml:"tmp-storage-quota" json:"tmp-storage-quota"` // Bytes
	// Deprecated
	EnableStreaming bool                    `toml:"-" json:"-"`
	EnableBatchDML  bool                    `toml:"enable-batch-dml" json:"enable-batch-dml"`
	TxnLocalLatches tikvcfg.TxnLocalLatches `toml:"-" json:"-"`
	// Set sys variable lower-case-table-names, ref: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html.
	// TODO: We actually only support mode 2, which keeps the original case, but the comparison is case-insensitive.
	LowerCaseTableNames        int                `toml:"lower-case-table-names" json:"lower-case-table-names"`
	ServerVersion              string             `toml:"server-version" json:"server-version"`
	Log                        Log                `toml:"log" json:"log"`
	Security                   Security           `toml:"security" json:"security"`
	Status                     Status             `toml:"status" json:"status"`
	Performance                Performance        `toml:"performance" json:"performance"`
	PreparedPlanCache          PreparedPlanCache  `toml:"prepared-plan-cache" json:"prepared-plan-cache"`
	OpenTracing                OpenTracing        `toml:"opentracing" json:"opentracing"`
	ProxyProtocol              ProxyProtocol      `toml:"proxy-protocol" json:"proxy-protocol"`
	PDClient                   tikvcfg.PDClient   `toml:"pd-client" json:"pd-client"`
	TiKVClient                 tikvcfg.TiKVClient `toml:"tikv-client" json:"tikv-client"`
	Binlog                     Binlog             `toml:"binlog" json:"binlog"`
	Inc                        Inc                `toml:"inc" json:"inc"`
	Osc                        Osc                `toml:"osc" json:"osc"`
	Ghost                      Ghost              `toml:"ghost" json:"ghost"`
	IncLevel                   IncLevel           `toml:"inc_level" json:"inc_level"`
	CompatibleKillQuery        bool               `toml:"compatible-kill-query" json:"compatible-kill-query"`
	Plugin                     Plugin             `toml:"plugin" json:"plugin"`
	PessimisticTxn             PessimisticTxn     `toml:"pessimistic-txn" json:"pessimistic-txn"`
	CheckMb4ValueInUTF8        bool               `toml:"check-mb4-value-in-utf8" json:"check-mb4-value-in-utf8"`
	MaxIndexLength             int                `toml:"max-index-length" json:"max-index-length"`
	IndexLimit                 int                `toml:"index-limit" json:"index-limit"`
	TableColumnCountLimit      uint32             `toml:"table-column-count-limit" json:"table-column-count-limit"`
	GracefulWaitBeforeShutdown int                `toml:"graceful-wait-before-shutdown" json:"graceful-wait-before-shutdown"`
	// AlterPrimaryKey is used to control alter primary key feature.
	AlterPrimaryKey bool `toml:"alter-primary-key" json:"alter-primary-key"`
	// TreatOldVersionUTF8AsUTF8MB4 is use to treat old version table/column UTF8 charset as UTF8MB4. This is for compatibility.
	// Currently not support dynamic modify, because this need to reload all old version schema.
	TreatOldVersionUTF8AsUTF8MB4 bool `toml:"treat-old-version-utf8-as-utf8mb4" json:"treat-old-version-utf8-as-utf8mb4"`
	// EnableTableLock indicate whether enable table lock.
	// TODO: remove this after table lock features stable.
	EnableTableLock     bool        `toml:"enable-table-lock" json:"enable-table-lock"`
	DelayCleanTableLock uint64      `toml:"delay-clean-table-lock" json:"delay-clean-table-lock"`
	SplitRegionMaxNum   uint64      `toml:"split-region-max-num" json:"split-region-max-num"`
	StmtSummary         StmtSummary `toml:"stmt-summary" json:"stmt-summary"`
	TopSQL              TopSQL      `toml:"top-sql" json:"top-sql"`
	// RepairMode indicates that the TiDB is in the repair mode for table meta.
	RepairMode      bool     `toml:"repair-mode" json:"repair-mode"`
	RepairTableList []string `toml:"repair-table-list" json:"repair-table-list"`
	// IsolationRead indicates that the TiDB reads data from which isolation level(engine and label).
	IsolationRead IsolationRead `toml:"isolation-read" json:"isolation-read"`
	// MaxServerConnections is the maximum permitted number of simultaneous client connections.
	MaxServerConnections uint32 `toml:"max-server-connections" json:"max-server-connections"`
	// NewCollationsEnabledOnFirstBootstrap indicates if the new collations are enabled, it effects only when a TiDB cluster bootstrapped on the first time.
	NewCollationsEnabledOnFirstBootstrap bool `toml:"new_collations_enabled_on_first_bootstrap" json:"new_collations_enabled_on_first_bootstrap"`
	// Experimental contains parameters for experimental features.
	Experimental Experimental `toml:"experimental" json:"experimental"`
	// EnableCollectExecutionInfo enables the TiDB to collect execution info.
	EnableCollectExecutionInfo bool `toml:"enable-collect-execution-info" json:"enable-collect-execution-info"`
	// SkipRegisterToDashboard tells TiDB don't register itself to the dashboard.
	SkipRegisterToDashboard bool `toml:"skip-register-to-dashboard" json:"skip-register-to-dashboard"`
	// EnableTelemetry enables the usage data report to PingCAP.
	EnableTelemetry bool `toml:"enable-telemetry" json:"enable-telemetry"`
	// Labels indicates the labels set for the tidb server. The labels describe some specific properties for the tidb
	// server like `zone`/`rack`/`host`. Currently, labels won't affect the tidb server except for some special
	// label keys. Now we have following special keys:
	// 1. 'group' is a special label key which should be automatically set by tidb-operator. We don't suggest
	// users to set 'group' in labels.
	// 2. 'zone' is a special key that indicates the DC location of this tidb-server. If it is set, the value for this
	// key will be the default value of the session variable `txn_scope` for this tidb-server.
	Labels map[string]string `toml:"labels" json:"labels"`
	// EnableGlobalIndex enables creating global index.
	EnableGlobalIndex bool `toml:"enable-global-index" json:"enable-global-index"`
	// DeprecateIntegerDisplayWidth indicates whether deprecating the max display length for integer.
	DeprecateIntegerDisplayWidth bool `toml:"deprecate-integer-display-length" json:"deprecate-integer-display-length"`
	// EnableEnumLengthLimit indicates whether the enum/set element length is limited.
	// According to MySQL 8.0 Refman:
	// The maximum supported length of an individual SET element is M <= 255 and (M x w) <= 1020,
	// where M is the element literal length and w is the number of bytes required for the maximum-length character in the character set.
	// See https://dev.mysql.com/doc/refman/8.0/en/string-type-syntax.html for more details.
	EnableEnumLengthLimit bool `toml:"enable-enum-length-limit" json:"enable-enum-length-limit"`
	// StoresRefreshInterval indicates the interval of refreshing stores info, the unit is second.
	StoresRefreshInterval uint64 `toml:"stores-refresh-interval" json:"stores-refresh-interval"`
	// EnableTCP4Only enables net.Listen("tcp4",...)
	// Note that: it can make lvs with toa work and thus tidb can get real client ip.
	EnableTCP4Only bool `toml:"enable-tcp4-only" json:"enable-tcp4-only"`
	// The client will forward the requests through the follower
	// if one of the following conditions happens:
	// 1. there is a network partition problem between TiDB and PD leader.
	// 2. there is a network partition problem between TiDB and TiKV leader.
	EnableForwarding bool `toml:"enable-forwarding" json:"enable-forwarding"`
	// MaxBallastObjectSize set the max size of the ballast object, the unit is byte.
	// The default value is the smallest of the following two values: 2GB or
	// one quarter of the total physical memory in the current system.
	MaxBallastObjectSize int `toml:"max-ballast-object-size" json:"max-ballast-object-size"`
	// BallastObjectSize set the initial size of the ballast object, the unit is byte.
	BallastObjectSize int `toml:"ballast-object-size" json:"ballast-object-size"`
}

// UpdateTempStoragePath is to update the `TempStoragePath` if port/statusPort was changed
// and the `tmp-storage-path` was not specified in the conf.toml or was specified the same as the default value.
func (c *Config) UpdateTempStoragePath() {
	if c.TempStoragePath == tempStorageDirName {
		c.TempStoragePath = encodeDefTempStorageDir(os.TempDir(), c.Host, c.Status.StatusHost, c.Port, c.Status.StatusPort)
	} else {
		c.TempStoragePath = encodeDefTempStorageDir(c.TempStoragePath, c.Host, c.Status.StatusHost, c.Port, c.Status.StatusPort)
	}
}

func (c *Config) getTiKVConfig() *tikvcfg.Config {
	return &tikvcfg.Config{
		CommitterConcurrency:  c.Performance.CommitterConcurrency,
		MaxTxnTTL:             c.Performance.MaxTxnTTL,
		TiKVClient:            c.TiKVClient,
		Security:              c.Security.ClusterSecurity(),
		PDClient:              c.PDClient,
		PessimisticTxn:        tikvcfg.PessimisticTxn{MaxRetryCount: c.PessimisticTxn.MaxRetryCount},
		TxnLocalLatches:       c.TxnLocalLatches,
		StoresRefreshInterval: c.StoresRefreshInterval,
		OpenTracingEnable:     c.OpenTracing.Enable,
		Path:                  c.Path,
		EnableForwarding:      c.EnableForwarding,
		TxnScope:              c.Labels["zone"],
	}
}

func encodeDefTempStorageDir(tempDir string, host, statusHost string, port, statusPort uint) string {
	dirName := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%v:%v/%v:%v", host, port, statusHost, statusPort)))
	var osUID string
	currentUser, err := user.Current()
	if err != nil {
		osUID = ""
	} else {
		osUID = currentUser.Uid
	}
	return filepath.Join(tempDir, osUID+"_tidb", dirName, "tmp-storage")
}

// nullableBool defaults unset bool options to unset instead of false, which enables us to know if the user has set 2
// conflict options at the same time.
type nullableBool struct {
	IsValid bool
	IsTrue  bool
}

var (
	nbUnset = nullableBool{false, false}
	nbFalse = nullableBool{true, false}
	nbTrue  = nullableBool{true, true}
)

func (b *nullableBool) toBool() bool {
	return b.IsValid && b.IsTrue
}

func (b nullableBool) MarshalJSON() ([]byte, error) {
	switch b {
	case nbTrue:
		return json.Marshal(true)
	case nbFalse:
		return json.Marshal(false)
	default:
		return json.Marshal(nil)
	}
}

func (b *nullableBool) UnmarshalText(text []byte) error {
	str := string(text)
	switch str {
	case "", "null":
		*b = nbUnset
		return nil
	case "true":
		*b = nbTrue
	case "false":
		*b = nbFalse
	default:
		*b = nbUnset
		return errors.New("Invalid value for bool type: " + str)
	}
	return nil
}

func (b nullableBool) MarshalText() ([]byte, error) {
	if !b.IsValid {
		return []byte(""), nil
	}
	if b.IsTrue {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (b *nullableBool) UnmarshalJSON(data []byte) error {
	var err error
	var v interface{}
	if err = json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch raw := v.(type) {
	case bool:
		*b = nullableBool{true, raw}
	default:
		*b = nbUnset
	}
	return err
}

// Log is the log section of config.
type Log struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format, one of json or text.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output. Deprecated: use EnableTimestamp instead.
	DisableTimestamp nullableBool `toml:"disable-timestamp" json:"disable-timestamp"`
	// EnableTimestamp enables automatic timestamps in log output.
	EnableTimestamp nullableBool `toml:"enable-timestamp" json:"enable-timestamp"`
	// DisableErrorStack stops annotating logs with the full stack error
	// message. Deprecated: use EnableErrorStack instead.
	DisableErrorStack nullableBool `toml:"disable-error-stack" json:"disable-error-stack"`
	// EnableErrorStack enables annotating logs with the full stack error
	// message.
	EnableErrorStack nullableBool `toml:"enable-error-stack" json:"enable-error-stack"`
	// File log config.
	File logutil.FileLogConfig `toml:"file" json:"file"`

	EnableSlowLog       bool   `toml:"enable-slow-log" json:"enable-slow-log"`
	SlowQueryFile       string `toml:"slow-query-file" json:"slow-query-file"`
	SlowThreshold       uint64 `toml:"slow-threshold" json:"slow-threshold"`
	ExpensiveThreshold  uint   `toml:"expensive-threshold" json:"expensive-threshold"`
	QueryLogMaxLen      uint64 `toml:"query-log-max-len" json:"query-log-max-len"`
	RecordPlanInSlowLog uint32 `toml:"record-plan-in-slow-log" json:"record-plan-in-slow-log"`
}

func (l *Log) getDisableTimestamp() bool {
	if l.EnableTimestamp == nbUnset && l.DisableTimestamp == nbUnset {
		return false
	}
	if l.EnableTimestamp == nbUnset {
		return l.DisableTimestamp.toBool()
	}
	return !l.EnableTimestamp.toBool()
}

func (l *Log) getDisableErrorStack() bool {
	if l.EnableErrorStack == nbUnset && l.DisableErrorStack == nbUnset {
		return true
	}
	if l.EnableErrorStack == nbUnset {
		return l.DisableErrorStack.toBool()
	}
	return !l.EnableErrorStack.toBool()
}

// The following constants represents the valid action configurations for Security.SpilledFileEncryptionMethod.
// "plaintext" means encryption is disabled.
// NOTE: Although the values is case insensitive, we should use lower-case
// strings because the configuration value will be transformed to lower-case
// string and compared with these constants in the further usage.
const (
	SpilledFileEncryptionMethodPlaintext = "plaintext"
	SpilledFileEncryptionMethodAES128CTR = "aes128-ctr"
)

// Security is the security section of the config.
type Security struct {
	SkipGrantTable         bool     `toml:"skip-grant-table" json:"skip-grant-table"`
	SSLCA                  string   `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert                string   `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey                 string   `toml:"ssl-key" json:"ssl-key"`
	RequireSecureTransport bool     `toml:"require-secure-transport" json:"require-secure-transport"`
	ClusterSSLCA           string   `toml:"cluster-ssl-ca" json:"cluster-ssl-ca"`
	ClusterSSLCert         string   `toml:"cluster-ssl-cert" json:"cluster-ssl-cert"`
	ClusterSSLKey          string   `toml:"cluster-ssl-key" json:"cluster-ssl-key"`
	ClusterVerifyCN        []string `toml:"cluster-verify-cn" json:"cluster-verify-cn"`
	// If set to "plaintext", the spilled files will not be encrypted.
	SpilledFileEncryptionMethod string `toml:"spilled-file-encryption-method" json:"spilled-file-encryption-method"`
	// EnableSEM prevents SUPER users from having full access.
	EnableSEM bool `toml:"enable-sem" json:"enable-sem"`
	// Allow automatic TLS certificate generation
	AutoTLS         bool   `toml:"auto-tls" json:"auto-tls"`
	MinTLSVersion   string `toml:"tls-version" json:"tls-version"`
	RSAKeySize      int    `toml:"rsa-key-size" json:"rsa-key-size"`
	SecureBootstrap bool   `toml:"secure-bootstrap" json:"secure-bootstrap"`
}

// The ErrConfigValidationFailed error is used so that external callers can do a type assertion
// to defer handling of this specific error when someone does not want strict type checking.
// This is needed only because logging hasn't been set up at the time we parse the config file.
// This should all be ripped out once strict config checking is made the default behavior.
type ErrConfigValidationFailed struct {
	confFile       string
	UndecodedItems []string
}

func (e *ErrConfigValidationFailed) Error() string {
	return fmt.Sprintf("config file %s contained invalid configuration options: %s; check "+
		"TiDB manual to make sure this option has not been deprecated and removed from your TiDB "+
		"version if the option does not appear to be a typo", e.confFile, strings.Join(
		e.UndecodedItems, ", "))
}

// ClusterSecurity returns Security info for cluster
func (s *Security) ClusterSecurity() tikvcfg.Security {
	return tikvcfg.NewSecurity(s.ClusterSSLCA, s.ClusterSSLCert, s.ClusterSSLKey, s.ClusterVerifyCN)
}

// Status is the status section of the config.
type Status struct {
	StatusHost      string `toml:"status-host" json:"status-host"`
	MetricsAddr     string `toml:"metrics-addr" json:"metrics-addr"`
	StatusPort      uint   `toml:"status-port" json:"status-port"`
	MetricsInterval uint   `toml:"metrics-interval" json:"metrics-interval"`
	ReportStatus    bool   `toml:"report-status" json:"report-status"`
	RecordQPSbyDB   bool   `toml:"record-db-qps" json:"record-db-qps"`
}

// Performance is the performance section of the config.
type Performance struct {
	MaxProcs uint `toml:"max-procs" json:"max-procs"`
	// Deprecated: use ServerMemoryQuota instead
	MaxMemory             uint64  `toml:"max-memory" json:"max-memory"`
	ServerMemoryQuota     uint64  `toml:"server-memory-quota" json:"server-memory-quota"`
	MemoryUsageAlarmRatio float64 `toml:"memory-usage-alarm-ratio" json:"memory-usage-alarm-ratio"`
	StatsLease            string  `toml:"stats-lease" json:"stats-lease"`
	StmtCountLimit        uint    `toml:"stmt-count-limit" json:"stmt-count-limit"`
	FeedbackProbability   float64 `toml:"feedback-probability" json:"feedback-probability"`
	QueryFeedbackLimit    uint    `toml:"query-feedback-limit" json:"query-feedback-limit"`
	PseudoEstimateRatio   float64 `toml:"pseudo-estimate-ratio" json:"pseudo-estimate-ratio"`
	ForcePriority         string  `toml:"force-priority" json:"force-priority"`
	BindInfoLease         string  `toml:"bind-info-lease" json:"bind-info-lease"`
	TxnEntrySizeLimit     uint64  `toml:"txn-entry-size-limit" json:"txn-entry-size-limit"`
	TxnTotalSizeLimit     uint64  `toml:"txn-total-size-limit" json:"txn-total-size-limit"`
	TCPKeepAlive          bool    `toml:"tcp-keep-alive" json:"tcp-keep-alive"`
	TCPNoDelay            bool    `toml:"tcp-no-delay" json:"tcp-no-delay"`
	CrossJoin             bool    `toml:"cross-join" json:"cross-join"`
	RunAutoAnalyze        bool    `toml:"run-auto-analyze" json:"run-auto-analyze"`
	DistinctAggPushDown   bool    `toml:"distinct-agg-push-down" json:"distinct-agg-push-down"`
	CommitterConcurrency  int     `toml:"committer-concurrency" json:"committer-concurrency"`
	MaxTxnTTL             uint64  `toml:"max-txn-ttl" json:"max-txn-ttl"`
	MemProfileInterval    string  `toml:"mem-profile-interval" json:"mem-profile-interval"`
	IndexUsageSyncLease   string  `toml:"index-usage-sync-lease" json:"index-usage-sync-lease"`
	PlanReplayerGCLease   string  `toml:"plan-replayer-gc-lease" json:"plan-replayer-gc-lease"`
	GOGC                  int     `toml:"gogc" json:"gogc"`
	EnforceMPP            bool    `toml:"enforce-mpp" json:"enforce-mpp"`
}

// PlanCache is the PlanCache section of the config.
type PlanCache struct {
	Enabled  bool `toml:"enabled" json:"enabled"`
	Capacity uint `toml:"capacity" json:"capacity"`
	Shards   uint `toml:"shards" json:"shards"`
}

// PreparedPlanCache is the PreparedPlanCache section of the config.
type PreparedPlanCache struct {
	Enabled          bool    `toml:"enabled" json:"enabled"`
	Capacity         uint    `toml:"capacity" json:"capacity"`
	MemoryGuardRatio float64 `toml:"memory-guard-ratio" json:"memory-guard-ratio"`
}

// OpenTracing is the opentracing section of the config.
type OpenTracing struct {
	Enable     bool                `toml:"enable" json:"enable"`
	RPCMetrics bool                `toml:"rpc-metrics" json:"rpc-metrics"`
	Sampler    OpenTracingSampler  `toml:"sampler" json:"sampler"`
	Reporter   OpenTracingReporter `toml:"reporter" json:"reporter"`
}

// OpenTracingSampler is the config for opentracing sampler.
// See https://godoc.org/github.com/uber/jaeger-client-go/config#SamplerConfig
type OpenTracingSampler struct {
	Type                    string        `toml:"type" json:"type"`
	Param                   float64       `toml:"param" json:"param"`
	SamplingServerURL       string        `toml:"sampling-server-url" json:"sampling-server-url"`
	MaxOperations           int           `toml:"max-operations" json:"max-operations"`
	SamplingRefreshInterval time.Duration `toml:"sampling-refresh-interval" json:"sampling-refresh-interval"`
}

// OpenTracingReporter is the config for opentracing reporter.
// See https://godoc.org/github.com/uber/jaeger-client-go/config#ReporterConfig
type OpenTracingReporter struct {
	QueueSize           int           `toml:"queue-size" json:"queue-size"`
	BufferFlushInterval time.Duration `toml:"buffer-flush-interval" json:"buffer-flush-interval"`
	LogSpans            bool          `toml:"log-spans" json:"log-spans"`
	LocalAgentHostPort  string        `toml:"local-agent-host-port" json:"local-agent-host-port"`
}

// ProxyProtocol is the PROXY protocol section of the config.
type ProxyProtocol struct {
	// PROXY protocol acceptable client networks.
	// Empty string means disable PROXY protocol,
	// * means all networks.
	Networks string `toml:"networks" json:"networks"`
	// PROXY protocol header read timeout, Unit is second.
	HeaderTimeout uint `toml:"header-timeout" json:"header-timeout"`
}

// Binlog is the config for binlog.
type Binlog struct {
	Enable bool `toml:"enable" json:"enable"`
	// If IgnoreError is true, when writing binlog meets error, TiDB would
	// ignore the error.
	IgnoreError  bool   `toml:"ignore-error" json:"ignore-error"`
	WriteTimeout string `toml:"write-timeout" json:"write-timeout"`
	// Use socket file to write binlog, for compatible with kafka version tidb-binlog.
	BinlogSocket string `toml:"binlog-socket" json:"binlog-socket"`
	// The strategy for sending binlog to pump, value can be "range" or "hash" now.
	Strategy string `toml:"strategy" json:"strategy"`
}

// Inc is the inception section of the config.
type Inc struct {
	AlterAutoMerge bool   `toml:"alter_auto_merge" json:"alter_auto_merge"`
	BackupHost     string `toml:"backup_host" json:"backup_host"` // 远程备份库信息
	BackupPassword string `toml:"backup_password" json:"backup_password"`
	BackupPort     uint   `toml:"backup_port" json:"backup_port"`
	BackupUser     string `toml:"backup_user" json:"backup_user"`
	// 指定backup mysql ssl认证规则,默认为空. 可指定tls=skip-verify等来跳过服务器ssl认证. https://github.com/go-sql-driver/mysql/issues/899#issuecomment-443493840
	BackupTLS string `toml:"backup_tls" json:"backup_tls"`

	CheckAutoIncrementDataType  bool `toml:"check_autoincrement_datatype" json:"check_autoincrement_datatype"`
	CheckAutoIncrementInitValue bool `toml:"check_autoincrement_init_value" json:"check_autoincrement_init_value"`
	CheckAutoIncrementName      bool `toml:"check_autoincrement_name" json:"check_autoincrement_name"`
	CheckColumnComment          bool `toml:"check_column_comment" json:"check_column_comment"`
	CheckColumnDefaultValue     bool `toml:"check_column_default_value" json:"check_column_default_value"`
	// 检查列顺序变更 #40
	CheckColumnPositionChange bool `toml:"check_column_position_change" json:"check_column_position_change"`
	// 检查列类型变更(允许长度变更,类型变更时警告)
	CheckColumnTypeChange       bool `toml:"check_column_type_change" json:"check_column_type_change"`
	CheckDMLLimit               bool `toml:"check_dml_limit" json:"check_dml_limit"`
	CheckDMLOrderBy             bool `toml:"check_dml_orderby" json:"check_dml_orderby"`
	CheckDMLWhere               bool `toml:"check_dml_where" json:"check_dml_where"`
	CheckIdentifier             bool `toml:"check_identifier" json:"check_identifier"`
	CheckImplicitTypeConversion bool `toml:"check_implicit_type_conversion"` // 检查where条件中的隐式类型转换
	CheckIndexPrefix            bool `toml:"check_index_prefix" json:"check_index_prefix"`
	CheckIndexColumnRepeat      bool `toml:"check_index_column_repeat" json:"check_index_column_repeat"`
	CheckInsertField            bool `toml:"check_insert_field" json:"check_insert_field"`
	CheckPrimaryKey             bool `toml:"check_primary_key" json:"check_primary_key"`
	CheckTableComment           bool `toml:"check_table_comment" json:"check_table_comment"`
	CheckTableRowSize           bool `toml:"check_table_size" json:"check_table_size"`
	CheckTimestampDefault       bool `toml:"check_timestamp_default" json:"check_timestamp_default"`
	CheckTimestampCount         bool `toml:"check_timestamp_count" json:"check_timestamp_count"`
	CheckToolBasedUniqueIndex   bool `toml:"check_tool_based_unique_index" json:"check_tool_based_unique_index"`

	EnableTimeStampType  bool `toml:"enable_timestamp_type" json:"enable_timestamp_type"`
	EnableZeroDate       bool `toml:"enable_zero_date" json:"enable_zero_date"`
	CheckDatetimeDefault bool `toml:"check_datetime_default" json:"check_datetime_default"`
	CheckDatetimeCount   bool `toml:"check_datetime_count" json:"check_datetime_count"`

	// 将 float/double 转成 decimal, 默认为 false
	CheckFloatDouble bool `toml:"check_float_double" json:"check_float_double"`

	CheckIdentifierUpper bool `toml:"check_identifier_upper" json:"check_identifier_upper"`
	CheckIdentifierLower bool `toml:"check_identifier_lower" json:"check_identifier_lower"`
	// 数据库是否只读的判断. 默认为true
	CheckReadOnly bool `toml:"check_read_only" json:"check_read_only"`

	// 连接服务器的默认字符集,默认值为utf8mb4
	DefaultCharset string `toml:"default_charset" json:"default_charset"`
	// 禁用数据库类型,多个时以逗号分隔.该参数优先级低于enable_blob_type/enable_enum_set_bit等参数
	DisableTypes        string `toml:"disable_types" json:"disable_types"`
	EnableAlterDatabase bool   `toml:"enable_alter_database" json:"enable_alter_database"`
	// 允许执行任意语法类型.该设置有安全要求,仅支持配置文件方式设置
	EnableAnyStatement          bool `toml:"enable_any_statement" json:"enable_any_statement"`
	EnableAutoIncrementUnsigned bool `toml:"enable_autoincrement_unsigned" json:"enable_autoincrement_unsigned"`
	// 允许blob,text,json列设置为NOT NULL
	EnableBlobNotNull   bool `toml:"enable_blob_not_null" json:"enable_blob_not_null"`
	EnableBlobType      bool `toml:"enable_blob_type" json:"enable_blob_type"`
	EnableChangeColumn  bool `toml:"enable_change_column" json:"enable_change_column"` // 允许change column操作
	EnableColumnCharset bool `toml:"enable_column_charset" json:"enable_column_charset"`
	EnableDropDatabase  bool `toml:"enable_drop_database" json:"enable_drop_database"`
	EnableDropTable     bool `toml:"enable_drop_table" json:"enable_drop_table"` // 允许删除表
	EnableEnumSetBit    bool `toml:"enable_enum_set_bit" json:"enable_enum_set_bit"`

	// DML指纹功能,开启后,在审核时,类似DML将直接复用审核结果,可大幅优化审核效率
	EnableFingerprint      bool `toml:"enable_fingerprint" json:"enable_fingerprint"`
	EnableForeignKey       bool `toml:"enable_foreign_key" json:"enable_foreign_key"`
	EnableIdentiferKeyword bool `toml:"enable_identifer_keyword" json:"enable_identifer_keyword"`
	EnableJsonType         bool `toml:"enable_json_type" json:"enable_json_type"`
	EnableUseView          bool `toml:"enable_use_view" json:"enable_use_view"`
	// 是否启用自定义审核级别设置
	// EnableLevel bool `toml:"enable_level" json:"enable_level"`
	// 是否启用最小化回滚SQL设置,当开启时,update语句中未变更的值不再记录到回滚语句中
	EnableMinimalRollback bool `toml:"enable_minimal_rollback" json:"enable_minimal_rollback"`
	// 是否允许指定存储引擎
	EnableSetEngine        bool `toml:"enable_set_engine" json:"enable_set_engine"`
	EnableNullable         bool `toml:"enable_nullable" json:"enable_nullable"`               // 允许空列
	EnableNullIndexName    bool `toml:"enable_null_index_name" json:"enable_null_index_name"` //是否允许不指定索引名
	EnableOrderByRand      bool `toml:"enable_orderby_rand" json:"enable_orderby_rand"`
	EnablePartitionTable   bool `toml:"enable_partition_table" json:"enable_partition_table"`
	EnablePKColumnsOnlyInt bool `toml:"enable_pk_columns_only_int" json:"enable_pk_columns_only_int"`
	EnableSelectStar       bool `toml:"enable_select_star" json:"enable_select_star"`
	EnableCreateProcedure  bool `toml:"enable_create_procedure" json:"enable_create_procedure"`
	EnableDropProcedure    bool `toml:"enable_drop_procedure" json:"enable_drop_procedure"`
	EnableCreateFunction   bool `toml:"enable_create_function" json:"enable_create_function"`
	EnableDropFunction     bool `toml:"enable_drop_function" json:"enable_drop_function"`
	EnableCreateTrigger    bool `toml:"enable_create_trigger" json:"enable_create_trigger"`
	EnableDropTrigger      bool `toml:"enable_drop_trigger" json:"enable_drop_trigger"`

	// 是否允许设置字符集和排序规则
	EnableSetCharset   bool `toml:"enable_set_charset" json:"enable_set_charset"`
	EnableSetCollation bool `toml:"enable_set_collation" json:"enable_set_collation"`
	// 开启sql统计
	EnableSqlStatistic bool `toml:"enable_sql_statistic" json:"enable_sql_statistic"`
	// 在MySQL8.0检测是否支持 ALGORITHM=INSTANT, 当支持时自动关闭pt-osc/gh-ost.
	EnableDDLInstant bool `toml:"enable_ddl_instant" json:"enable_ddl_instant"`
	// explain判断受影响行数时使用的规则, 默认值"first"
	// 可选值: "first", "max"
	// 		"first": 	使用第一行的explain结果作为受影响行数
	// 		"max": 		使用explain结果中的最大值作为受影响行数
	ExplainRule string `toml:"explain_rule" json:"explain_rule"`

	// sql_mode, 默认值""
	// 可选值: "", "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "TRADITIONAL"
	//      "":                         默认使用目标 MySQL 实例 sql_mode
	//      "STRICT_TRANS_TABLES":      为事务性存储引擎以及可能的情况下为非事务性存储引擎启用严格的SQL模式
	//      "STRICT_ALL_TABLES":        为所有存储引擎启用严格的SQL模式
	//      "TRADITIONAL":              严格的SQL组合模式, 相当于STRICT_TRANS_TABLES， STRICT_ALL_TABLES， NO_ZERO_IN_DATE， NO_ZERO_DATE， ERROR_FOR_DIVISION_BY_ZERO， NO_AUTO_CREATE_USER，和 NO_ENGINE_SUBSTITUTION
	SqlMode string `toml:"sql_mode" json:"sql_mode"`

	// 全量日志
	GeneralLog bool `toml:"general_log" json:"general_log"`
	// 使用十六进制表示法转储二进制列
	// 受影响的数据类型为BINARY，VARBINARY，BLOB类型
	HexBlob bool `toml:"hex_blob" json:"hex_blob"`

	// 表名/索引名前缀，为空时不作限制
	IndexPrefix     string `toml:"index_prefix" json:"index_prefix"`
	UniqIndexPrefix string `toml:"uniq_index_prefix" json:"uniq_index_prefix"`
	TablePrefix     string `toml:"table_prefix" json:"table_prefix"`

	Lang string `toml:"lang" json:"lang"`
	// 连接服务器允许的最大包大小,以字节为单位 默认值为4194304(即4MB)
	MaxAllowedPacket uint `toml:"max_allowed_packet" json:"max_allowed_packet"`
	MaxCharLength    uint `toml:"max_char_length" json:"max_char_length"`
	// 单表的列数上限
	MaxColumnCount uint `toml:"max_column_count" json:"max_column_count"`
	// DDL操作最大允许的受影响行数. 默认值0,即不限制
	MaxDDLAffectRows uint `toml:"max_ddl_affect_rows" json:"max_ddl_affect_rows"`

	// 一次最多写入的行数, 仅判断insert values语法
	MaxInsertRows uint `toml:"max_insert_rows" json:"max_insert_rows"`

	MaxKeys       uint `toml:"max_keys" json:"max_keys"`
	MaxKeyParts   uint `toml:"max_key_parts" json:"max_key_parts"`
	MaxUpdateRows uint `toml:"max_update_rows" json:"max_update_rows"`
	// varchar类型的最大长度，当超出时添加警告
	MaxVarcharLength   uint `toml:"max_varchar_length" json:"max_varchar_length"`
	MaxPrimaryKeyParts uint `toml:"max_primary_key_parts" json:"max_primary_key_parts"` // 主键最多允许有几列组合
	MergeAlterTable    bool `toml:"merge_alter_table" json:"merge_alter_table"`

	// 建表必须创建的列. 可指定多个列,以逗号分隔.列类型可选. 格式: 列名 [列类型,可选],...
	MustHaveColumns string `toml:"must_have_columns" json:"must_have_columns"`
	// 如果表包含以下列，列必须有索引。可指定多个列,以逗号分隔.列类型可选.   格式: 列名 [列类型,可选],...
	ColumnsMustHaveIndex string `toml:"columns_must_have_index" json:"columns_must_have_index"`

	// 是否跳过用户权限校验
	SkipGrantTable bool `toml:"skip_grant_table" json:"skip_grant_table"`
	// 要跳过的sql语句, 多个时以分号分隔
	SkipSqls string `toml:"skip_sqls" json:"skip_sqls"`

	// alter table子句忽略OSC工具.
	// 格式为drop index,add column等,配置要跳过的子句格式,多个时以逗号分隔
	IgnoreOscAlterStmt string `toml:"ignore_osc_alter_stmt" json:"ignore_osc_alter_stmt"`

	// 安全更新是否开启.
	// -1 表示不做操作,基于远端数据库 [默认值]
	// 0  表示关闭安全更新
	// 1  表示开启安全更新
	SqlSafeUpdates int `toml:"sql_safe_updates" json:"sql_safe_updates"`

	// 设置执行SQL时，会话变量
	// 0 表示不做操作，基于远端数据库【默认值】
	// > 0 值表示，会话在执行SQL 时获取锁超时的时间
	LockWaitTimeout int `toml:"lock_wait_timeout" json:"lock_wait_timeout"`

	// 支持的字符集
	SupportCharset string `toml:"support_charset" json:"support_charset"`

	// 支持的排序规则
	SupportCollation string `toml:"support_collation" json:"support_collation"`
	// Version *string

	// 支持的存储引擎,多个时以分号分隔
	SupportEngine string `toml:"support_engine" json:"support_engine"`
	// 远端数据库等待超时时间, 单位:秒
	WaitTimeout int `toml:"wait_timeout" json:"wait_timeout"`

	// 远端数据库最大执行时间, 单位:秒
	MaxExecutionTime int `toml:"max_execution_time" json:"max_execution_time"`
	// 版本信息
	Version string `toml:"version" json:"version"`

	// 自定义的关键字，用于检查字段名是否符合规范
	CustomKeywords []string `toml:"custom_keywords" json:"custom_keywords"`
}

// Osc online schema change 工具参数配置
type Osc struct {

	// 用来设置在arkit返回结果集中，对于原来OSC在执行过程的标准输出信息是不是要打印到结果集对应的错误信息列中，
	// 如果设置为1，就不打印，如果设置为0，就打印。而如果出现了错误，则都会打印。默认值：OFF
	OscPrintNone bool `toml:"osc_print_none" json:"osc_print_none"`

	// 对应参数pt-online-schema-change中的参数--print。默认值：OFF
	OscPrintSql bool `toml:"osc_print_sql" json:"osc_print_sql"`

	// 全局的OSC开关，默认是打开的，如果想要关闭则设置为OFF，这样就会直接修改。默认值：OFF
	OscOn bool `toml:"osc_on" json:"osc_on"`

	// 这个参数实际上是一个OSC开关，如果设置为0，则全部ALTER语句都使用OSC方式，
	// 如果设置为非0，则当这个表占用空间大小大于这个值时才使用OSC方式。
	// 单位为M，这个表大小的计算方式是通过语句
	// select (DATA_LENGTH + INDEX_LENGTH)/1024/1024 from information_schema.tables
	// where table_schema = 'dbname' and table_name = 'tablename' 来实现的。默认值：16
	// [0-1048576]
	OscMinTableSize uint `toml:"osc_min_table_size" json:"osc_min_table_size"`

	// 对应参数pt-online-schema-change中的参数alter-foreign-keys-method，具体意义可以参考OSC官方手册。默认值：none
	// [auto | none | rebuild_constraints | drop_swap]
	OscAlterForeignKeysMethod string `toml:"osc_alter_foreign_keys_method" json:"osc_alter_foreign_keys_method"`

	// 对应参数pt-online-schema-change中的参数recursion_method，具体意义可以参考OSC官方手册。默认值：processlist
	// [processlist | hosts | none]
	OscRecursionMethod string `toml:"osc_recursion_method" json:"osc_recursion_method"`

	// 对应参数pt-online-schema-change中的参数--max-lag。默认值：3
	OscMaxLag int `toml:"osc_max_lag" json:"osc_max_lag"`

	// 类似--max-lag，检查集群暂停流量控制所花费的平均时间（仅适用于PXC 5.6及以上版本）
	OscMaxFlowCtl int `toml:"osc_max_flow_ctl" json:"osc_max_flow_ctl"`

	// 对应参数pt-online-schema-change中的参数--[no]check-alter。默认值：ON
	OscCheckAlter bool `toml:"osc_check_alter" json:"osc_check_alter"`

	// 对应参数pt-online-schema-change中的参数 --sleep 默认值：0.0
	OscSleep float32 `toml:"osc_sleep" json:"osc_sleep"`

	// 对应参数pt-online-schema-change中的参数 --set-vars lock_wait_timeout=60s
	OscLockWaitTimeout int `toml:"osc_lock_wait_timeout" json:"osc_lock_wait_timeout"`

	// 对应参数pt-online-schema-change中的参数--[no]check-replication-filters。默认值：ON
	OscCheckReplicationFilters bool `toml:"osc_check_replication_filters" json:"osc_check_replication_filters"`
	// 是否检查唯一索引,默认检查,如果是,则禁止
	OscCheckUniqueKeyChange bool `toml:"osc_check_unique_key_change" json:"osc_check_unique_key_change"`

	// 对应参数pt-online-schema-change中的参数--[no]drop-old-table。默认值：ON
	OscDropOldTable bool `toml:"osc_drop_old_table" json:"osc_drop_old_table"`

	// 对应参数pt-online-schema-change中的参数--[no]drop-new-table。默认值：ON
	OscDropNewTable bool `toml:"osc_drop_new_table" json:"osc_drop_new_table"`

	// 对应参数pt-online-schema-change中的参数--max-load中的thread_running部分。默认值：80
	OscMaxThreadRunning int `toml:"osc_max_thread_running" json:"osc_max_thread_running"`

	// 对应参数pt-online-schema-change中的参数--max-load中的thread_connected部分。默认值：1000
	OscMaxThreadConnected int `toml:"osc_max_thread_connected" json:"osc_max_thread_connected"`

	// 对应参数pt-online-schema-change中的参数--critical-load中的thread_running部分。默认值：80
	OscCriticalThreadRunning int `toml:"osc_critical_thread_running" json:"osc_critical_thread_running"`

	// 对应参数pt-online-schema-change中的参数--critical-load中的thread_connected部分。默认值：1000
	OscCriticalThreadConnected int `toml:"osc_critical_thread_connected" json:"osc_critical_thread_connected"`

	// 对应参数pt-online-schema-change中的参数--chunk-time。默认值：1
	OscChunkTime float32 `toml:"osc_chunk_time" json:"osc_chunk_time"`

	// 对应参数pt-online-schema-change中的参数--chunk-size-limit。默认值：4
	OscChunkSizeLimit int `toml:"osc_chunk_size_limit" json:"osc_chunk_size_limit"`

	// 对应参数pt-online-schema-change中的参数--chunk-size。默认值：1000
	OscChunkSize int `toml:"osc_chunk_size" json:"osc_chunk_size"`

	// 对应参数pt-online-schema-change中的参数--check-interval，意义是Sleep time between checks for --max-lag。默认值：5
	OscCheckInterval int `toml:"osc_check_interval" json:"osc_check_interval"`

	OscBinDir string `toml:"osc_bin_dir" json:"osc_bin_dir"`
}

// GhOst online schema change 工具参数配置
type Ghost struct {

	// 阿里云数据库
	GhostAliyunRds bool `toml:"ghost_aliyun_rds"`
	// 允许gh-ost运行在双主复制架构中，一般与-assume-master-host参数一起使用。
	GhostAllowMasterMaster bool `toml:"ghost_allow_master_master"`
	// 允许gh-ost在数据迁移(migrate)依赖的唯一键可以为NULL，默认为不允许为NULL的唯一键。如果数据迁移(migrate)依赖的唯一键允许NULL值，则可能造成数据不正确，请谨慎使用。
	GhostAllowNullableUniqueKey bool `toml:"ghost_allow_nullable_unique_key"`
	// 允许gh-ost直接运行在主库上。默认gh-ost连接的从库。
	GhostAllowOnMaster bool `toml:"ghost_allow_on_master"`

	// 如果你修改一个列的名字(如change column)，gh-ost将会识别到并且需要提供重命名列名的原因，默认情况下gh-ost是不继续执行的，除非提供-approve-renamed-columns ALTER。
	// ALTER
	GhostApproveRenamedColumns bool `toml:"ghost_approve_renamed_columns"`
	// 为gh-ost指定一个主库，格式为"ip:port"或者"hostname:port"。默认推荐gh-ost连接从库。
	GhostAssumeMasterHost string `toml:"ghost_assume_master_host"`
	// 确认gh-ost连接的数据库实例的binlog_format=ROW的情况下，可以指定-assume-rbr，
	// 这样可以禁止从库上运行stop slave,start slave,执行gh-ost用户也不需要SUPER权限。
	GhostAssumeRbr         bool `toml:"ghost_assume_rbr"`
	GhostAttemptInstantDDL bool `toml:"ghost_attempt_instant_ddl"`
	// 该参数如果为True(默认值)，则进行row-copy之后，估算统计行数(使用explain select count(*)方式)，
	// 并调整ETA时间，否则，gh-ost首先预估统计行数，然后开始row-copy。
	GhostConcurrentRowcount bool `toml:"ghost_concurrent_rowcount"`

	// 一系列逗号分隔的status-name=values组成，当MySQL中status超过对应的values，gh-ost将会退出
	// 	e.g:
	// -critical-load Threads_connected=20,Connections=1500
	// 指的是当MySQL中的状态值Threads_connected>20,Connections>1500的时候，gh-ost将会由于该数据库严重负载而停止并退出。
	// GhostCriticalLoad string `toml:"ghost_critical_load"`

	// 当值为0时，当达到-critical-load，gh-ost立即退出。当值不为0时，当达到-critical-load，
	// gh-ost会在-critical-load-interval-millis秒数后，再次进行检查，再次检查依旧达到-critical-load，gh-ost将会退出。
	GhostCriticalLoadIntervalMillis   int64 `toml:"ghost_critical_load_interval_millis"`
	GhostCriticalLoadHibernateSeconds int64 `toml:"ghost_critical_load_hibernate_seconds"`

	// 选择cut-over类型:atomic/two-step，atomic(默认)类型的cut-over是github的算法，two-step采用的是facebook-OSC的算法。
	GhostCutOver string `toml:"ghost_cut_over"`

	GhostCutOverExponentialBackoff bool `toml:"ghost_cut_over_exponential_backoff"`
	// 在每次迭代中处理的行数量(允许范围：100-100000)，默认值为1000。
	GhostChunkSize int64 `toml:"ghost_chunk_size"`

	// gh-ost在cut-over阶段最大的锁等待时间，当锁超时时，gh-ost的cut-over将重试。(默认值：3)
	GhostCutOverLockTimeoutSeconds int64 `toml:"ghost_cut_over_lock_timeout_seconds"`

	// 很危险的参数，慎用！
	// 该参数针对一个有外键的表，在gh-ost创建ghost表时，并不会为ghost表创建外键。该参数很适合用于删除外键，除此之外，请谨慎使用。
	GhostDiscardForeignKeys bool `toml:"ghost_discard_foreign_keys"`

	// 各种操作在panick前重试次数。(默认为60)
	GhostDefaultRetries int64 `toml:"ghost_default_retries"`

	GhostDmlBatchSize int64 `toml:"ghost_dml_batch_size"`

	// 准确统计表行数(使用select count(*)的方式)，得到更准确的预估时间。
	GhostExactRowcount bool `toml:"ghost_exact_rowcount"`

	// 实际执行alter&migrate表，默认为不执行，仅仅做测试并退出，如果想要ALTER TABLE语句真正落实到数据库中去，需要明确指定-execute
	// GhostExecute bool `toml:"ghost_execute"`

	GhostExponentialBackoffMaxInterval int64 `toml:"ghost_exponential_backoff_max_interval"`
	// When true, the 'unpostpone|cut-over' interactive command must name the migrated table。
	GhostForceTableNames   string `toml:"ghost_force_table_names"`
	GhostForceNamedCutOver bool   `toml:"ghost_force_named_cut_over"`
	GhostGcp               bool   `toml:"ghost_gcp"`

	// gh-ost心跳频率值，默认为500。
	GhostHeartbeatIntervalMillis int64 `toml:"ghost_heartbeat_interval_millis"`

	// gh-ost操作之前，检查并删除已经存在的ghost表。该参数不建议使用，请手动处理原来存在的ghost表。默认不启用该参数，gh-ost直接退出操作。
	GhostInitiallyDropGhostTable bool `toml:"ghost_initially_drop_ghost_table"`

	// gh-ost操作之前，检查并删除已经存在的旧表。该参数不建议使用，请手动处理原来存在的ghost表。默认不启用该参数，gh-ost直接退出操作。
	GhostInitiallyDropOldTable bool `toml:"ghost_initially_drop_old_table"`

	// gh-ost强制删除已经存在的socket文件。该参数不建议使用，可能会删除一个正在运行的gh-ost程序，导致DDL失败。
	GhostInitiallyDropSocketFile bool `toml:"ghost_initially_drop_socket_file"`

	// 主从复制最大延迟时间，当主从复制延迟时间超过该值后，gh-ost将采取节流(throttle)措施，默认值：1500s。
	GhostMaxLagMillis int64 `toml:"ghost_max_lag_millis"`

	// 	一系列逗号分隔的status-name=values组成，当MySQL中status超过对应的values，gh-ost将采取节流(throttle)措施。
	// e.g:
	// -max-load Threads_connected=20,Connections=1500
	// 指的是当MySQL中的状态值Threads_connected>20,Connections>1500的时候，gh-ost将采取节流(throttle)措施。
	// GhostMaxLoad string `toml:"ghost_max_load"`

	GhostNiceRatio float64 `toml:"ghost_nice_ratio"`
	// gh-ost的数据迁移(migrate)运行在从库上，而不是主库上。
	// Have the migration run on a replica, not on the master.
	// This will do the full migration on the replica including cut-over (as opposed to --test-on-replica)
	// GhostMigrateOnReplica bool `toml:"ghost_migrate_on_replica"`

	// 开启标志
	GhostOn bool `toml:"ghost_on"`
	// gh-ost操作结束后，删除旧表，默认状态是不删除旧表，会存在_tablename_del表。
	GhostOkToDropTable bool `toml:"ghost_ok_to_drop_table"`
	// 当这个文件存在的时候，gh-ost的cut-over阶段将会被推迟，直到该文件被删除。
	GhostPostponeCutOverFlagFile string `toml:"ghost_postpone_cut_over_flag_file"`
	// GhostPanicFlagFile           string `toml:"ghost_panic_flag_file"`
	// GhostReplicaServerID      bool `toml:"ghost_replica_server_id"`
	GhostSkipForeignKeyChecks bool `toml:"ghost_skip_foreign_key_checks"`

	// 如果你修改一个列的名字(如change column)，gh-ost将会识别到并且需要提供重命名列名的原因，
	// 默认情况下gh-ost是不继续执行的。该参数告诉gh-ost跳该列的数据迁移，
	// 让gh-ost把重命名列作为无关紧要的列。该操作很危险，你会损失该列的所有值。
	// GhostSkipRenamedColumns bool `toml:"ghost_skip_renamed_columns"`

	// GhostSsl              bool `toml:"ghost_ssl"`
	// GhostSslAllowInsecure bool `toml:"ghost_ssl_allow_insecure"`
	// GhostSslCa            bool `toml:"ghost_ssl_ca"`

	// 在从库上测试gh-ost，包括在从库上数据迁移(migration)，数据迁移完成后stop slave，
	// 原表和ghost表立刻交换而后立刻交换回来。继续保持stop slave，使你可以对比两张表。
	// GhostTestOnReplica bool `toml:"ghost_test_on_replica"`

	// 当-test-on-replica执行时，该参数表示该过程中不用stop slave。
	// GhostTestOnReplicaSkipReplicaStop bool `toml:"ghost_test_on_replica_skip_replica_stop"`

	// 	列出所有需要被检查主从复制延迟的从库。
	// e.g:
	// -throttle-control-replica=192.16.12.22:3306,192.16.12.23:3307,192.16.13.12:3308
	GhostThrottleControlReplicas    string `toml:"ghost_throttle_control_replicas"`
	GhostThrottleHTTP               string `toml:"ghost_throttle_http"`
	GhostTimestampOldTable          bool   `toml:"ghost_timestamp_old_table"`
	GhostThrottleQuery              string `toml:"ghost_throttle_query"`
	GhostThrottleFlagFile           string `toml:"ghost_throttle_flag_file"`
	GhostThrottleAdditionalFlagFile string `toml:"ghost_throttle_additional_flag_file"`

	// 告诉gh-ost你正在运行的是一个tungsten-replication拓扑结构。
	GhostTungsten            bool   `toml:"ghost_tungsten"`
	GhostReplicationLagQuery string `toml:"ghost_replication_lag_query"`

	// gh-ost所在目录，当设置该参数时，则启用二进制gh-ost
	GhostBinDir string `toml:"ghost_bin_dir" json:"ghost_bin_dir"`
}

// PessimisticTxn is the config for pessimistic transaction.
type PessimisticTxn struct {
	// The max count of retry for a single statement in a pessimistic transaction.
	MaxRetryCount uint `toml:"max-retry-count" json:"max-retry-count"`
	// The max count of deadlock events that will be recorded in the information_schema.deadlocks table.
	DeadlockHistoryCapacity uint `toml:"deadlock-history-capacity" json:"deadlock-history-capacity"`
	// Whether retryable deadlocks (in-statement deadlocks) are collected to the information_schema.deadlocks table.
	DeadlockHistoryCollectRetryable bool `toml:"deadlock-history-collect-retryable" json:"deadlock-history-collect-retryable"`
}

// DefaultPessimisticTxn returns the default configuration for PessimisticTxn
func DefaultPessimisticTxn() PessimisticTxn {
	return PessimisticTxn{
		MaxRetryCount:                   256,
		DeadlockHistoryCapacity:         10,
		DeadlockHistoryCollectRetryable: false,
	}
}

// Plugin is the config for plugin
type Plugin struct {
	Dir  string `toml:"dir" json:"dir"`
	Load string `toml:"load" json:"load"`
}

// StmtSummary is the config for statement summary.
type StmtSummary struct {
	// Enable statement summary or not.
	Enable bool `toml:"enable" json:"enable"`
	// Enable summary internal query.
	EnableInternalQuery bool `toml:"enable-internal-query" json:"enable-internal-query"`
	// The maximum number of statements kept in memory.
	MaxStmtCount uint `toml:"max-stmt-count" json:"max-stmt-count"`
	// The maximum length of displayed normalized SQL and sample SQL.
	MaxSQLLength uint `toml:"max-sql-length" json:"max-sql-length"`
	// The refresh interval of statement summary.
	RefreshInterval int `toml:"refresh-interval" json:"refresh-interval"`
	// The maximum history size of statement summary.
	HistorySize int `toml:"history-size" json:"history-size"`
}

// TopSQL is the config for TopSQL.
type TopSQL struct {
	// The TopSQL's data receiver address.
	ReceiverAddress string `toml:"receiver-address" json:"receiver-address"`
}

// IsolationRead is the config for isolation read.
type IsolationRead struct {
	// Engines filters tidb-server access paths by engine type.
	Engines []string `toml:"engines" json:"engines"`
}

// Experimental controls the features that are still experimental: their semantics, interfaces are subject to change.
// Using these features in the production environment is not recommended.
type Experimental struct {
	// Whether enable creating expression index.
	AllowsExpressionIndex bool `toml:"allow-expression-index" json:"allow-expression-index"`
	// Whether enable global kill.
	EnableGlobalKill bool `toml:"enable-global-kill" json:"-"`
	// Whether enable charset feature.
	EnableNewCharset bool `toml:"enable-new-charset" json:"-"`
}

var defTiKVCfg = tikvcfg.DefaultConfig()
var defaultConf = Config{
	Host:                         DefHost,
	AdvertiseAddress:             "",
	Port:                         DefPort,
	SecondPort:                   DefSecondPort,
	Socket:                       "/tmp/tidb-{Port}.sock",
	Cors:                         "",
	Store:                        "unistore",
	Path:                         "/tmp/tidb",
	RunDDL:                       true,
	SplitTable:                   true,
	Lease:                        "45s",
	TokenLimit:                   1000,
	OOMUseTmpStorage:             true,
	TempStorageQuota:             -1,
	TempStoragePath:              tempStorageDirName,
	OOMAction:                    OOMActionCancel,
	MemQuotaQuery:                1 << 30,
	EnableStreaming:              false,
	EnableBatchDML:               false,
	CheckMb4ValueInUTF8:          true,
	MaxIndexLength:               3072,
	IndexLimit:                   64,
	TableColumnCountLimit:        1017,
	AlterPrimaryKey:              false,
	TreatOldVersionUTF8AsUTF8MB4: true,
	EnableTableLock:              false,
	DelayCleanTableLock:          0,
	SplitRegionMaxNum:            1000,
	RepairMode:                   false,
	RepairTableList:              []string{},
	MaxServerConnections:         0,
	TxnLocalLatches:              defTiKVCfg.TxnLocalLatches,
	LowerCaseTableNames:          2,
	GracefulWaitBeforeShutdown:   0,
	ServerVersion:                "",
	Log: Log{
		Level:               "info",
		Format:              "text",
		File:                logutil.NewFileLogConfig(logutil.DefaultLogMaxSize),
		SlowQueryFile:       "tidb-slow.log",
		SlowThreshold:       logutil.DefaultSlowThreshold,
		ExpensiveThreshold:  10000,
		DisableErrorStack:   nbUnset,
		EnableErrorStack:    nbUnset, // If both options are nbUnset, getDisableErrorStack() returns true
		EnableTimestamp:     nbUnset,
		DisableTimestamp:    nbUnset, // If both options are nbUnset, getDisableTimestamp() returns false
		QueryLogMaxLen:      logutil.DefaultQueryLogMaxLen,
		RecordPlanInSlowLog: logutil.DefaultRecordPlanInSlowLog,
		EnableSlowLog:       logutil.DefaultTiDBEnableSlowLog,
	},
	Status: Status{
		ReportStatus:    true,
		StatusHost:      DefStatusHost,
		StatusPort:      DefStatusPort,
		MetricsInterval: 15,
		RecordQPSbyDB:   false,
	},
	Performance: Performance{
		MaxMemory:             0,
		ServerMemoryQuota:     0,
		MemoryUsageAlarmRatio: 0.8,
		TCPKeepAlive:          true,
		TCPNoDelay:            true,
		CrossJoin:             true,
		StatsLease:            "3s",
		RunAutoAnalyze:        true,
		StmtCountLimit:        5000,
		FeedbackProbability:   0.0,
		QueryFeedbackLimit:    512,
		PseudoEstimateRatio:   0.8,
		ForcePriority:         "NO_PRIORITY",
		BindInfoLease:         "3s",
		TxnEntrySizeLimit:     DefTxnEntrySizeLimit,
		TxnTotalSizeLimit:     DefTxnTotalSizeLimit,
		DistinctAggPushDown:   false,
		CommitterConcurrency:  defTiKVCfg.CommitterConcurrency,
		MaxTxnTTL:             defTiKVCfg.MaxTxnTTL, // 1hour
		MemProfileInterval:    "1m",
		// TODO: set indexUsageSyncLease to 60s.
		IndexUsageSyncLease: "0s",
		GOGC:                100,
		EnforceMPP:          false,
		PlanReplayerGCLease: "10m",
	},
	ProxyProtocol: ProxyProtocol{
		Networks:      "",
		HeaderTimeout: 5,
	},
	PreparedPlanCache: PreparedPlanCache{
		Enabled:          false,
		Capacity:         1000,
		MemoryGuardRatio: 0.1,
	},
	OpenTracing: OpenTracing{
		Enable: false,
		Sampler: OpenTracingSampler{
			Type:  "const",
			Param: 1.0,
		},
		Reporter: OpenTracingReporter{},
	},
	PDClient:   defTiKVCfg.PDClient,
	TiKVClient: defTiKVCfg.TiKVClient,
	Binlog: Binlog{
		WriteTimeout: "15s",
		Strategy:     "range",
	},
	Inc: Inc{
		EnableZeroDate:        true,
		EnableNullable:        true,
		EnableDropTable:       false,
		EnableSetEngine:       true,
		CheckTableComment:     false,
		CheckColumnComment:    false,
		EnableAnyStatement:    false,
		EnableChangeColumn:    true,
		CheckTimestampCount:   true,
		EnableTimeStampType:   true,
		CheckFloatDouble:      false,
		CheckIdentifierUpper:  false,
		CheckIdentifierLower:  false,
		CheckReadOnly:         true,
		EnableDDLInstant:      true,
		SqlSafeUpdates:        -1,
		LockWaitTimeout:       -1,
		SupportCharset:        "utf8,utf8mb4",
		SupportEngine:         "innodb",
		Lang:                  "en-US",
		CheckColumnTypeChange: true,

		// 连接服务器选项
		DefaultCharset:   "utf8mb4",
		MaxAllowedPacket: 4194304,
		ExplainRule:      "first",
		SqlMode:          "",

		// 为配置方便,在config节点也添加相同参数
		SkipGrantTable: true,

		// 默认参数不指定,避免test时失败
		// Version:            mysql.TiDBReleaseVersion,

		IndexPrefix:     "idx_",  // 默认不检查,由CheckIndexPrefix控制
		UniqIndexPrefix: "uniq_", // 默认不检查,由CheckIndexPrefix控制
		TablePrefix:     "",      // 默认不检查表前缀

		CustomKeywords: []string{},
	},
	Osc: Osc{
		OscPrintNone:               false,
		OscPrintSql:                false,
		OscOn:                      false,
		OscMinTableSize:            16,
		OscAlterForeignKeysMethod:  "none",
		OscRecursionMethod:         "processlist",
		OscMaxLag:                  3,
		OscMaxFlowCtl:              -1,
		OscSleep:                   0.0,
		OscLockWaitTimeout:         60,
		OscCheckAlter:              true,
		OscCheckReplicationFilters: true,
		OscCheckUniqueKeyChange:    true,
		OscDropOldTable:            true,
		OscDropNewTable:            true,
		OscMaxThreadRunning:        80,
		OscMaxThreadConnected:      1000,
		OscCriticalThreadRunning:   80,
		OscCriticalThreadConnected: 1000,
		OscChunkTime:               1,
		OscChunkSizeLimit:          4,
		OscChunkSize:               1000,
		OscCheckInterval:           5,
		OscBinDir:                  "/usr/local/bin",
	},
	Ghost: Ghost{
		GhostOn:                            false,
		GhostAllowOnMaster:                 true,
		GhostAssumeRbr:                     true,
		GhostAttemptInstantDDL:             false,
		GhostChunkSize:                     1000,
		GhostConcurrentRowcount:            true,
		GhostCutOver:                       "atomic",
		GhostCutOverLockTimeoutSeconds:     3,
		GhostDefaultRetries:                60,
		GhostHeartbeatIntervalMillis:       500,
		GhostMaxLagMillis:                  1500,
		GhostApproveRenamedColumns:         true,
		GhostExponentialBackoffMaxInterval: 64,
		GhostDmlBatchSize:                  10,
		GhostOkToDropTable:                 true,
		GhostSkipForeignKeyChecks:          true,
		GhostTimestampOldTable:             false,
	},
	IncLevel: defaultLevel,
	Plugin: Plugin{
		Dir:  "/data/deploy/plugin",
		Load: "",
	},
	PessimisticTxn: DefaultPessimisticTxn(),
	StmtSummary: StmtSummary{
		Enable:              true,
		EnableInternalQuery: false,
		MaxStmtCount:        3000,
		MaxSQLLength:        4096,
		RefreshInterval:     1800,
		HistorySize:         24,
	},
	IsolationRead: IsolationRead{
		Engines: []string{"tikv", "tiflash", "tidb"},
	},
	Experimental: Experimental{
		EnableGlobalKill: false,
		EnableNewCharset: false,
	},
	EnableCollectExecutionInfo: true,
	EnableTelemetry:            true,
	Labels:                     make(map[string]string),
	EnableGlobalIndex:          false,
	Security: Security{
		SpilledFileEncryptionMethod: SpilledFileEncryptionMethodPlaintext,
		EnableSEM:                   false,
		AutoTLS:                     false,
		RSAKeySize:                  4096,
	},
	DeprecateIntegerDisplayWidth: false,
	EnableEnumLengthLimit:        true,
	StoresRefreshInterval:        defTiKVCfg.StoresRefreshInterval,
	EnableForwarding:             defTiKVCfg.EnableForwarding,
}

var (
	globalConf atomic.Value
)

// NewConfig creates a new config instance with default value.
func NewConfig() *Config {
	conf := defaultConf
	return &conf
}

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	return globalConf.Load().(*Config)
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
	cfg := *config.getTiKVConfig()
	tikvcfg.StoreGlobalConfig(&cfg)
}

var deprecatedConfig = map[string]struct{}{
	"pessimistic-txn.ttl":            {},
	"pessimistic-txn.enable":         {},
	"log.file.log-rotate":            {},
	"log.log-slow-query":             {},
	"txn-local-latches":              {},
	"txn-local-latches.enabled":      {},
	"txn-local-latches.capacity":     {},
	"performance.max-memory":         {},
	"max-txn-time-use":               {},
	"experimental.allow-auto-random": {},
	"enable-redact-log":              {}, // use variable tidb_redact_log instead
	"tikv-client.copr-cache.enable":  {},
	"alter-primary-key":              {}, // use NONCLUSTERED keyword instead
	"enable-streaming":               {},
}

func isAllDeprecatedConfigItems(items []string) bool {
	for _, item := range items {
		if _, ok := deprecatedConfig[item]; !ok {
			return false
		}
	}
	return true
}

// IsMemoryQuotaQuerySetByUser indicates whether the config item mem-quota-query
// is set by the user.
var IsMemoryQuotaQuerySetByUser bool

// IsOOMActionSetByUser indicates whether the config item mem-action is set by
// the user.
var IsOOMActionSetByUser bool

// InitializeConfig initialize the global config handler.
// The function enforceCmdArgs is used to merge the config file with command arguments:
// For example, if you start TiDB by the command "./tidb-server --port=3000", the port number should be
// overwritten to 3000 and ignore the port number in the config file.
func InitializeConfig(confPath string, configCheck, configStrict bool, enforceCmdArgs func(*Config)) {
	cfg := GetGlobalConfig()
	var err error
	if confPath != "" {
		if err = cfg.Load(confPath); err != nil {
			// Unused config item error turns to warnings.
			if tmp, ok := err.(*ErrConfigValidationFailed); ok {
				// This block is to accommodate an interim situation where strict config checking
				// is not the default behavior of TiDB. The warning message must be deferred until
				// logging has been set up. After strict config checking is the default behavior,
				// This should all be removed.
				if (!configCheck && !configStrict) || isAllDeprecatedConfigItems(tmp.UndecodedItems) {
					fmt.Fprintln(os.Stderr, err.Error())
					err = nil
				}
			}
		}

		terror.MustNil(err)
	} else {
		// configCheck should have the config file specified.
		if configCheck {
			fmt.Fprintln(os.Stderr, "config check failed", errors.New("no config file specified for config-check"))
			os.Exit(1)
		}
	}
	enforceCmdArgs(cfg)

	if err := cfg.Valid(); err != nil {
		if !filepath.IsAbs(confPath) {
			if tmp, err := filepath.Abs(confPath); err == nil {
				confPath = tmp
			}
		}
		fmt.Fprintln(os.Stderr, "load config file:", confPath)
		fmt.Fprintln(os.Stderr, "invalid config", err)
		os.Exit(1)
	}
	if configCheck {
		fmt.Println("config check successful")
		os.Exit(0)
	}
	StoreGlobalConfig(cfg)
}

// Load loads config options from a toml file.
func (c *Config) Load(confFile string) error {
	metaData, err := toml.DecodeFile(confFile, c)
	if c.TokenLimit == 0 {
		c.TokenLimit = 1000
	}
	if metaData.IsDefined("mem-quota-query") {
		IsMemoryQuotaQuerySetByUser = true
	}
	if metaData.IsDefined("oom-action") {
		IsOOMActionSetByUser = true
	}
	// If any items in confFile file are not mapped into the Config struct, issue
	// an error and stop the server from starting.
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		err = &ErrConfigValidationFailed{confFile, undecodedItems}
	}

	return err
}

// Valid checks if this config is valid.
func (c *Config) Valid() error {
	if c.Log.EnableErrorStack == c.Log.DisableErrorStack && c.Log.EnableErrorStack != nbUnset {
		logutil.BgLogger().Warn(fmt.Sprintf("\"enable-error-stack\" (%v) conflicts \"disable-error-stack\" (%v). \"disable-error-stack\" is deprecated, please use \"enable-error-stack\" instead. disable-error-stack is ignored.", c.Log.EnableErrorStack, c.Log.DisableErrorStack))
		// if two options conflict, we will use the value of EnableErrorStack
		c.Log.DisableErrorStack = nbUnset
	}
	if c.Log.EnableTimestamp == c.Log.DisableTimestamp && c.Log.EnableTimestamp != nbUnset {
		logutil.BgLogger().Warn(fmt.Sprintf("\"enable-timestamp\" (%v) conflicts \"disable-timestamp\" (%v). \"disable-timestamp\" is deprecated, please use \"enable-timestamp\" instead", c.Log.EnableTimestamp, c.Log.DisableTimestamp))
		// if two options conflict, we will use the value of EnableTimestamp
		c.Log.DisableTimestamp = nbUnset
	}
	if c.Security.SkipGrantTable && !hasRootPrivilege() {
		return fmt.Errorf("TiDB run with skip-grant-table need root privilege")
	}
	if !ValidStorage[c.Store] {
		nameList := make([]string, 0, len(ValidStorage))
		for k, v := range ValidStorage {
			if v {
				nameList = append(nameList, k)
			}
		}
		return fmt.Errorf("invalid store=%s, valid storages=%v", c.Store, nameList)
	}
	if c.Store == "mocktikv" && !c.RunDDL {
		return fmt.Errorf("can't disable DDL on mocktikv")
	}
	if c.MaxIndexLength < DefMaxIndexLength || c.MaxIndexLength > DefMaxOfMaxIndexLength {
		return fmt.Errorf("max-index-length should be [%d, %d]", DefMaxIndexLength, DefMaxOfMaxIndexLength)
	}
	if c.IndexLimit < DefIndexLimit || c.IndexLimit > DefMaxOfIndexLimit {
		return fmt.Errorf("index-limit should be [%d, %d]", DefIndexLimit, DefMaxOfIndexLimit)
	}
	if c.Log.File.MaxSize > MaxLogFileSize {
		return fmt.Errorf("invalid max log file size=%v which is larger than max=%v", c.Log.File.MaxSize, MaxLogFileSize)
	}
	c.OOMAction = strings.ToLower(c.OOMAction)
	if c.OOMAction != OOMActionLog && c.OOMAction != OOMActionCancel {
		return fmt.Errorf("unsupported OOMAction %v, TiDB only supports [%v, %v]", c.OOMAction, OOMActionLog, OOMActionCancel)
	}
	if c.TableColumnCountLimit < DefTableColumnCountLimit || c.TableColumnCountLimit > DefMaxOfTableColumnCountLimit {
		return fmt.Errorf("table-column-limit should be [%d, %d]", DefIndexLimit, DefMaxOfTableColumnCountLimit)
	}

	// lower_case_table_names is allowed to be 0, 1, 2
	if c.LowerCaseTableNames < 0 || c.LowerCaseTableNames > 2 {
		return fmt.Errorf("lower-case-table-names should be 0 or 1 or 2")
	}

	// txn-local-latches
	if err := c.TxnLocalLatches.Valid(); err != nil {
		return err
	}

	// For tikvclient.
	if err := c.TiKVClient.Valid(); err != nil {
		return err
	}

	if c.Performance.TxnTotalSizeLimit > 1<<40 {
		return fmt.Errorf("txn-total-size-limit should be less than %d", 1<<40)
	}

	if c.Performance.MemoryUsageAlarmRatio > 1 || c.Performance.MemoryUsageAlarmRatio < 0 {
		return fmt.Errorf("memory-usage-alarm-ratio in [Performance] must be greater than or equal to 0 and less than or equal to 1")
	}

	if c.StmtSummary.MaxStmtCount <= 0 {
		return fmt.Errorf("max-stmt-count in [stmt-summary] should be greater than 0")
	}
	if c.StmtSummary.HistorySize < 0 {
		return fmt.Errorf("history-size in [stmt-summary] should be greater than or equal to 0")
	}
	if c.StmtSummary.RefreshInterval <= 0 {
		return fmt.Errorf("refresh-interval in [stmt-summary] should be greater than 0")
	}

	if c.PreparedPlanCache.Capacity < 1 {
		return fmt.Errorf("capacity in [prepared-plan-cache] should be at least 1")
	}
	if c.PreparedPlanCache.MemoryGuardRatio < 0 || c.PreparedPlanCache.MemoryGuardRatio > 1 {
		return fmt.Errorf("memory-guard-ratio in [prepared-plan-cache] must be NOT less than 0 and more than 1")
	}
	if len(c.IsolationRead.Engines) < 1 {
		return fmt.Errorf("the number of [isolation-read]engines for isolation read should be at least 1")
	}
	for _, engine := range c.IsolationRead.Engines {
		if engine != "tidb" && engine != "tikv" && engine != "tiflash" {
			return fmt.Errorf("type of [isolation-read]engines can't be %v should be one of tidb or tikv or tiflash", engine)
		}
	}

	// test security
	c.Security.SpilledFileEncryptionMethod = strings.ToLower(c.Security.SpilledFileEncryptionMethod)
	switch c.Security.SpilledFileEncryptionMethod {
	case SpilledFileEncryptionMethodPlaintext, SpilledFileEncryptionMethodAES128CTR:
	default:
		return fmt.Errorf("unsupported [security]spilled-file-encryption-method %v, TiDB only supports [%v, %v]",
			c.Security.SpilledFileEncryptionMethod, SpilledFileEncryptionMethodPlaintext, SpilledFileEncryptionMethodAES128CTR)
	}

	// test log level
	l := zap.NewAtomicLevel()
	return l.UnmarshalText([]byte(c.Log.Level))
}

// UpdateGlobal updates the global config, and provide a restore function that can be used to restore to the original.
func UpdateGlobal(f func(conf *Config)) {
	g := GetGlobalConfig()
	newConf := *g
	f(&newConf)
	StoreGlobalConfig(&newConf)
}

// RestoreFunc gets a function that restore the config to the current value.
func RestoreFunc() (restore func()) {
	g := GetGlobalConfig()
	return func() {
		StoreGlobalConfig(g)
	}
}

func hasRootPrivilege() bool {
	return os.Geteuid() == 0
}

// TableLockEnabled uses to check whether enabled the table lock feature.
func TableLockEnabled() bool {
	return GetGlobalConfig().EnableTableLock
}

// TableLockDelayClean uses to get the time of delay clean table lock.
var TableLockDelayClean = func() uint64 {
	return GetGlobalConfig().DelayCleanTableLock
}

// ToLogConfig converts *Log to *logutil.LogConfig.
func (l *Log) ToLogConfig() *logutil.LogConfig {
	return logutil.NewLogConfig(l.Level, l.Format, l.SlowQueryFile, l.File, l.getDisableTimestamp(), func(config *zaplog.Config) { config.DisableErrorVerbose = l.getDisableErrorStack() })
}

// ToTracingConfig converts *OpenTracing to *tracing.Configuration.
func (t *OpenTracing) ToTracingConfig() *tracing.Configuration {
	ret := &tracing.Configuration{
		Disabled:   !t.Enable,
		RPCMetrics: t.RPCMetrics,
		Reporter:   &tracing.ReporterConfig{},
		Sampler:    &tracing.SamplerConfig{},
	}
	ret.Reporter.QueueSize = t.Reporter.QueueSize
	ret.Reporter.BufferFlushInterval = t.Reporter.BufferFlushInterval
	ret.Reporter.LogSpans = t.Reporter.LogSpans
	ret.Reporter.LocalAgentHostPort = t.Reporter.LocalAgentHostPort

	ret.Sampler.Type = t.Sampler.Type
	ret.Sampler.Param = t.Sampler.Param
	ret.Sampler.SamplingServerURL = t.Sampler.SamplingServerURL
	ret.Sampler.MaxOperations = t.Sampler.MaxOperations
	ret.Sampler.SamplingRefreshInterval = t.Sampler.SamplingRefreshInterval
	return ret
}

func init() {
	initByLDFlags(versioninfo.TiDBEdition, checkBeforeDropLDFlag)
}

func initByLDFlags(edition, checkBeforeDropLDFlag string) {
	if edition != versioninfo.CommunityEdition {
		defaultConf.EnableTelemetry = false
	}
	conf := defaultConf
	StoreGlobalConfig(&conf)
	if checkBeforeDropLDFlag == "1" {
		CheckTableBeforeDrop = true
	}
}

// The following constants represents the valid action configurations for OOMAction.
// NOTE: Although the values is case insensitive, we should use lower-case
// strings because the configuration value will be transformed to lower-case
// string and compared with these constants in the further usage.
const (
	OOMActionCancel = "cancel"
	OOMActionLog    = "log"
)

// hideConfig is used to filter a single line of config for hiding.
var hideConfig = []string{
	"index-usage-sync-lease",
}

// HideConfig is used to filter the configs that needs to be hidden.
func HideConfig(s string) string {
	configs := strings.Split(s, "\n")
	hideMap := make([]bool, len(configs))
	for i, c := range configs {
		for _, hc := range hideConfig {
			if strings.Contains(c, hc) {
				hideMap[i] = true
				break
			}
		}
	}
	var buf bytes.Buffer
	for i, c := range configs {
		if hideMap[i] {
			continue
		}
		if i != 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(c)
	}
	return buf.String()
}

// ContainHiddenConfig checks whether it contains the configuration that needs to be hidden.
func ContainHiddenConfig(s string) bool {
	s = strings.ToLower(s)
	for _, hc := range hideConfig {
		if strings.Contains(s, hc) {
			return true
		}
	}
	return false
}
