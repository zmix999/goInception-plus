package session

import (
	"fmt"
	"strings"

	"gitee.com/zhoujin826/goInception-plus/parser/mysql"
	"gitee.com/zhoujin826/goInception-plus/util"
	"github.com/jinzhu/gorm"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

func (s *session) PostgreSQLCheckOptions() error {

	if s.opt == nil {
		return errors.New("未配置数据源信息!")
	}

	if s.opt.split || s.opt.Check || s.opt.Print {
		s.opt.Execute = false
		s.opt.Backup = false

		// 审核阶段自动忽略警告,以免审核过早中止
		s.opt.IgnoreWarnings = true
	}

	if s.opt.sleep <= 0 {
		s.opt.sleepRows = 0
	} else if s.opt.sleepRows < 1 {
		s.opt.sleepRows = 1
	}

	if s.opt.split || s.opt.Print {
		s.opt.Check = false
	}

	// 不再检查密码是否为空
	if s.opt.Host == "" || s.opt.Port == 0 || s.opt.User == "" {
		log.Warningf("%#v", s.opt)
		msg := ""
		if s.opt.Host == "" {
			msg += "主机名为空,"
		}
		if s.opt.Port == 0 {
			msg += "端口为0,"
		}
		if s.opt.User == "" {
			msg += "用户名为空,"
		}
		return fmt.Errorf(s.getErrorMessage(ER_SQL_INVALID_SOURCE), strings.TrimRight(msg, ","))
	}

	var addr string
	if s.opt.middlewareExtend == "" && s.opt.db != "" {
		addr = fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=disable",
			s.opt.User, s.opt.Password, s.opt.Host, s.opt.Port, s.opt.db)
	} else {
		s.opt.middlewareExtend = fmt.Sprintf("/*%s*/",
			strings.Replace(s.opt.middlewareExtend, ": ", "=", 1))

		addr = fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=postgres sslmode=disable",
			s.opt.User, s.opt.Password, s.opt.Host, s.opt.Port)
	}

	db, err := gorm.Open("postgres", addr)

	if err != nil {
		return fmt.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
	}

	// 禁用日志记录器，不显示任何日志
	db.LogMode(false)

	s.db = db

	s.dbName = s.opt.db

	if s.opt.Backup {
		// 不再检查密码是否为空
		if s.inc.BackupHost == "" || s.inc.BackupPort == 0 || s.inc.BackupUser == "" || s.inc.BackupDb == "" {
			return errors.New(s.getErrorMessage(ER_INVALID_BACKUP_HOST_INFO))
		}
		s.inintbackupdb()
	}

	tmp := s.processInfo.Load()
	if tmp != nil {
		pi := tmp.(*util.ProcessInfo)
		pi.DestHost = s.opt.Host
		pi.DestPort = s.opt.Port
		pi.DestUser = s.opt.User

		if s.opt.Check {
			pi.Command = mysql.ComCheck
		} else if s.opt.Execute {
			pi.Command = mysql.ComExecute
		}
		s.processInfo.Store(pi)
	}

	s.PostgreSQLServerVersion()

	if s.opt.tranBatch > 1 {
		s.ddlDB, _ = gorm.Open("postgres", addr)
		s.ddlDB.LogMode(false)
	}
	return nil
}
