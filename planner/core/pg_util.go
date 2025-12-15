package core

import "gitee.com/zhoujin826/goInception-plus/expression"

// GetTableSchema 获取insert计划的表结构
func (s *Insert) GetTableSchema() *expression.Schema {
	return s.tableSchema
}
