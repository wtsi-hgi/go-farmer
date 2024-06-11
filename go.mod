module github.com/wtsi-hgi/go-farmer

go 1.22.0

require github.com/elastic/go-elasticsearch/v7 v7.17.10

require (
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/ugorji/go/codec v1.2.12
	go.etcd.io/bbolt v1.3.10
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/sys v0.4.0 // indirect
