// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"database/sql"

	pgDriver "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// 判断是否为无效连接错误
func isInvalidConnError(err error) bool {
	pgErr, ok := err.(*pgDriver.Error)
	if !ok {
		return false
	}
	if pgErr.Code == "08006" {
		return true
	}
	return false
}

// raw 执行sql语句,连接失败时自动重连,自动重置当前数据库
func (s *session) PostgreSQLraw(sqlStr string) (rows *sql.Rows, err error) {
	// 连接断开无效时,自动重试
	for i := 0; i < maxBadConnRetries; i++ {
		rows, err = s.db.DB().Query(sqlStr)
		if err == nil {
			return
		}
		// 检查是否是语法错误（PostgreSQL 的错误码为 '42601'）
		if pgErr, ok := err.(*pgDriver.Error); ok && pgErr.Code == "42601" {
			log.Errorf("Syntax error in query: %v", err)
			return nil, err
		} else {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		}
		// 如果是无效连接错误，则尝试重新初始化连接
		if isInvalidConnError(err) {
			err1 := s.initConnection()
			if err1 != nil {
				return nil, err1
			}
			continue
		}
		return
	}
	return
}

// exec 执行sql语句,连接失败时自动重连,自动重置当前数据库
func (s *session) PostgreSQLexecSQL(sqlStr string, retry bool) (res sql.Result, err error) {
	log.Debug("exec")
	// 连接断开无效时,自动重试
	for i := 0; i < maxBadConnRetries; i++ {
		res, err = s.db.DB().Exec(sqlStr)
		if err == nil {
			return
		}
		log.Errorf("con:%d [retry:%v] %v sql:%s",
			s.sessionVars.ConnectionID, i, err, sqlStr)

		if isInvalidConnError(err) {
			err1 := s.initConnection()
			if err1 != nil {
				return res, err1
			}
			if retry {
				s.appendWarningMessage(err.Error())
				continue
			}
			return
		}
		return
	}
	return
}

// execDDL 执行sql语句,连接失败时自动重连,自动重置当前数据库
func (s *session) PostgreSQLexecDDL(sqlStr string, retry bool) (res sql.Result, err error) {
	log.Debug("execDDL")
	// 连接断开无效时,自动重试
	for i := 0; i < maxBadConnRetries; i++ {
		res, err = s.ddlDB.DB().Exec(sqlStr)
		if err == nil {
			return
		}
		log.Errorf("con:%d %v sql:%s", s.sessionVars.ConnectionID, err, sqlStr)
		if isInvalidConnError(err) {
			err1 := s.initConnection()
			if err1 != nil {
				return res, err1
			}
			if retry {
				s.appendWarningMessage(err.Error())
				continue
			}
			return
		}
		return
	}
	return
}

// Raw 执行sql语句,连接失败时自动重连,自动重置当前数据库
func (s *session) PostgreSQLrawScan(sqlStr string, dest interface{}) (err error) {
	// 连接断开无效时,自动重试
	for i := 0; i < maxBadConnRetries; i++ {
		err = s.db.Raw(sqlStr).Scan(dest).Error
		if err == nil {
			return
		}
		if isInvalidConnError(err) {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			err1 := s.initConnection()
			if err1 != nil {
				return err1
			}
			s.appendErrorMsg(err.Error())
			continue
		}
		return
	}
	return
}

// Raw 执行sql语句,连接失败时自动重连,自动重置当前数据库
func (s *session) PostgreSQLrawDB(dest interface{}, sqlStr string, values ...interface{}) (err error) {
	// 连接断开无效时,自动重试
	for i := 0; i < maxBadConnRetries; i++ {
		err = s.db.Raw(sqlStr, values...).Scan(dest).Error
		if err == nil {
			return
		}
		if isInvalidConnError(err) {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			err1 := s.initConnection()
			if err1 != nil {
				return err1
			}
			s.appendErrorMsg(err.Error())
			continue
		}
		return
	}
	return
}

// initConnection 连接失败时自动重连,重连后重置当前数据库
func (s *session) PostgreSQLinitConnection() (err error) {
	log.Debug("PostgreSQLinitConnection")
	name := s.dbName
	if name == "" {
		name = s.opt.db
	}

	// 连接断开无效时,自动重试
	for i := 0; i < maxBadConnRetries; i++ {
		if name == "" {
			err = s.db.DB().Ping()
		}
		if err == nil {
			// 连接重连时,清除线程ID缓存
			// s.threadID = 0
			log.Infof("con:%d Database timeout reconnect", s.sessionVars.ConnectionID)
			return
		}

		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if !isInvalidConnError(err) {
			if myErr, ok := err.(*pgDriver.Error); ok {
				s.appendErrorMsg(myErr.Message)
			} else {
				s.appendErrorMsg(err.Error())
			}
			return
		}
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*pgDriver.Error); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err.Error())
		}
	}
	return
}
