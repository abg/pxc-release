module github.com/cloudfoundry-incubator/switchboard

go 1.20

require (
	code.cloudfoundry.org/lager/v3 v3.0.1
	code.cloudfoundry.org/tlsconfig v0.0.0-20230320190829-8f91c367795b
	github.com/cloudfoundry-incubator/galera-healthcheck v0.0.0-20220901215914-d591811a0fba
	github.com/maxbrunsfeld/counterfeiter/v6 v6.6.1
	github.com/onsi/ginkgo/v2 v2.9.5
	github.com/onsi/gomega v1.27.6
	github.com/pivotal-cf-experimental/service-config v0.0.0-20160129003516-b1dc94de6ada
	github.com/tedsuo/ifrit v0.0.0-20230330192023-5cba443a66c4
	gopkg.in/validator.v2 v2.0.1
)

require (
	filippo.io/edwards25519 v1.0.0 // indirect
	github.com/bmizerany/pat v0.0.0-20210406213842-e4b6760bdd6f // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/pprof v0.0.0-20230510103437-eeec1cb781c3 // indirect
	github.com/imdario/mergo v0.3.15 // indirect
	github.com/openzipkin/zipkin-go v0.4.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/square/certstrap v1.3.0 // indirect
	github.com/tedsuo/rata v1.0.0 // indirect
	go.step.sm/crypto v0.30.0 // indirect
	golang.org/x/crypto v0.9.0 // indirect
	golang.org/x/mod v0.10.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	golang.org/x/tools v0.9.1 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/cloudfoundry-incubator/galera-healthcheck => ../galera-healthcheck
