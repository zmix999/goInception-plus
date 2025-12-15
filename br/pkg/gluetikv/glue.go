// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetikv

import (
	"context"

	"gitee.com/zhoujin826/goInception-plus/br/pkg/glue"
	"gitee.com/zhoujin826/goInception-plus/br/pkg/summary"
	"gitee.com/zhoujin826/goInception-plus/br/pkg/utils"
	"gitee.com/zhoujin826/goInception-plus/br/pkg/version/build"
	"gitee.com/zhoujin826/goInception-plus/config"
	"gitee.com/zhoujin826/goInception-plus/domain"
	"gitee.com/zhoujin826/goInception-plus/kv"
	"gitee.com/zhoujin826/goInception-plus/store/driver"
	pd "github.com/tikv/pd/client"
)

// Glue is an implementation of glue.Glue that accesses only TiKV without TiDB.
type Glue struct{}

// GetDomain implements glue.Glue.
func (Glue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}

// CreateSession implements glue.Glue.
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	return nil, nil
}

// Open implements glue.Glue.
func (Glue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	if option.CAPath != "" {
		conf := config.GetGlobalConfig()
		conf.Security.ClusterSSLCA = option.CAPath
		conf.Security.ClusterSSLCert = option.CertPath
		conf.Security.ClusterSSLKey = option.KeyPath
		config.StoreGlobalConfig(conf)
	}
	return driver.TiKVDriver{}.Open(path)
}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return utils.StartProgress(ctx, cmdName, total, redirectLog, nil)
}

// Record implements glue.Glue.
func (Glue) Record(name string, val uint64) {
	summary.CollectSuccessUnit(name, 1, val)
}

// GetVersion implements glue.Glue.
func (Glue) GetVersion() string {
	return "BR\n" + build.Info()
}
