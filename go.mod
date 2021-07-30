module github.com/pengdaqian2020/tidb-lite

go 1.12

require (
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/golang/snappy v0.0.1 // indirect
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/pingcap/check v0.0.0-20191107115940-caf2b9e6ccf4
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/log v0.0.0-20191012051959-b742a5d432e9
	github.com/pingcap/parser v0.0.0-20191210055545-753e13bfdbf0
	github.com/pingcap/tidb v1.1.0-beta.0.20191211070559-a94cff903cd1
	go.uber.org/zap v1.12.0
)

replace github.com/pingcap/parser v0.0.0-20191210055545-753e13bfdbf0 => github.com/WangXiangUSTC/parser v0.0.0-20210518141443-57a13baea68a

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
