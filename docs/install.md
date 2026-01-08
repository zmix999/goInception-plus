

### Install package


[goInception package](https://github.com/zmix999/goInception/releases)


### Source code 

- Golang v1.12 and above
- Go mod to manage package dependencies

```sh

# download source code
git clone https://github.com/zmix999/goInception-plus.git

cd goInception

make parser

# build package
go build -o goInception-plus tidb-server/main.go

```

#### start with configuration file

```sh
./goInception-plus -config=config/config.toml
```
