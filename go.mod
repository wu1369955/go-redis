module github.com/redis/go-redis/v9

go 1.23.0

require (
	github.com/bsm/ginkgo/v2 v2.12.0
	github.com/bsm/gomega v1.27.10
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f
)

require (
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/onsi/gomega v1.38.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v9.7.2 // This version was accidentally released. Please use version 9.7.3 instead.
	v9.5.4 // This version was accidentally released. Please use version 9.6.0 instead.
	v9.5.3 // This version was accidentally released. Please use version 9.6.0 instead.
)
