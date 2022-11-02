module github.com/GaussKernel

go 1.17

require (
	gitee.com/opengauss/openGauss-connector-go-pq v0.0.0-00010101000000-000000000000
	github.com/go-xorm/xorm v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
	xorm.io/builder v0.3.6 // indirect
	xorm.io/core v0.7.2-0.20190928055935-90aeac8d08eb // indirect
)

//replace gitee.com/opengauss/openGauss-connector-go-pq => ../../gitee.com/opengauss/openGauss-connector-go-pq

replace github.com/go-xorm/xorm => ../../github.com/go-xorm/xorm

replace gitee.com/opengauss/openGauss-connector-go-pq => ../../gitee.com/opengauss/openGauss-connector-go-pq
