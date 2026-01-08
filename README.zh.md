# goInception-plus

goInception-plus是一个集审核、执行、备份及生成回滚语句于一身的MySQL,PostgreSQL运维工具， 通过对执行SQL的语法解析，返回基于自定义规则的审核结果，并提供执行和备份及生成回滚语句的功能


----

### 安装说明


#### 源码编译

***go version 1.22 (go mod)***

```bash
git clone https://github.com/zmix999/goInception-plus.git
cd goInception-plus
go build -o goInception-plus tidb-server/main.go

./goInception-plus -config=config/config.toml
```

----

#### 致谢
    goInception-plus基于TiDB的语法解析器。
- [goInception - 审核工具](https://github.com/hanchuanchuan/goInception)
- [TiDB](https://github.com/pingcap/tidb)