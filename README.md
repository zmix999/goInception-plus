# goInception-plus

**[[English]](README.md)**
**[[Chinese]](README.zh.md)**

goInception-plus is a MySQL,PostgreSQL maintenance tool, which can be used to review, implement, backup, and generate SQL statements for rollback. It parses SQL syntax and returns the result of the review based on custom rules.


#### Source code compilation

***go version 1.22+ (go mod)***

```bash
git clone https://github.com/zmix999/goInception-plus.git
cd goInception-plus
go build -o goInception-plus tidb-server/main.go

./goInception-plus -config=config/config.toml
```

----

#### Associated SQL audit platform

* [Archery](https://github.com/hhyo/Archery) `Query support (MySQL/MsSQL/Redis/PostgreSQL), MySQL optimization (SQLAdvisor|SOAR|SQLTuning), slow log management, table structure comparison, session management, Alibaba Cloud RDS management, etc.`


#### Acknowledgments
    GoInception reconstructs from the Inception which is a well-known MySQL auditing tool and uses TiDB SQL parser.

- [goInception - 审核工具](https://github.com/hanchuanchuan/goInception)
- [TiDB](https://github.com/pingcap/tidb)

### Contributing

Welcome and thank you very much for your contribution. For the process of submitting PR, please refer to [CONTRIBUTING.md](CONTRIBUTING.md)。