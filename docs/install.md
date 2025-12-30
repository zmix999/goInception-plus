

### Install package


[goInception package](https://gitee.com/zhoujin826/goInception/releases)


### Source code 

- Golang v1.12 and above
- Go mod to manage package dependencies

```sh

# download source code
git clone https://gitee.com/zhoujin826/goInception-plus.git

cd goInception

make parser

# build package
go build -o goInception-plus tidb-server/main.go

```

#### start with configuration file

```sh
./goInception-plus -config=config/config.toml
```
