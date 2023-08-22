golangci_version = v1.53.3
goimports_version = v0.12.0
yamlfmt_version = v0.9.0

all: vet test

clean:
	rm -rf gen

generate: clean
	mkdir -p gen; cd gen; go run ../cmd/generate-fix/generate-fix.go ../spec/*.xml
#	go get -u all 

generate_maps: generate
	cd gen
	$(if $(shell which stringer-augmented),,$(error "No stringer-augmented in PATH, install from gopaca))
	cd enum && stringer-augmented -type `grep type enums.generated.go | awk '{print $$2}' | xargs | sed -e 's/ /,/g'` -sqlfile enums-generated-maps.sql -output enums.generated-reverse.go enums.generated.go

generate-dist:
	go run cmd/generate-fix/generate-fix.go spec/*.xml

generate-dist-win:
	go run cmd/generate-fix/generate-fix.go spec/FIX42.xml spec/FIX44.xml

format:
	go run golang.org/x/tools/cmd/goimports@${goimports_version} -w .
	go run github.com/google/yamlfmt/cmd/yamlfmt@${yamlfmt_version}

fmt:
	gofmt -l -w -s $(shell find . -type f -name '*.go')

vet:
	go vet . ./config ./datadictionary ./enum ./field ./internal ./tag
	go vet ./cmd/generate-fix ./cmd/generate-fix/internal
	go vet ./_test

lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@${golangci_version} run

lint-fix:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@${golangci_version} run --fix

test: 
	MONGODB_TEST_CXN=mongodb://db:27017 go test -v -cover -p=1 -count=1 . ./datadictionary ./internal

test-local:
	export MONGODB_TEST_CXN="mongodb://localhost:27017"
	go test -v -cover -p=1 -count=1 . ./datadictionary ./internal

_build_all: 
	go build -v . ./config ./datadictionary ./enum ./field ./fix42 ./fix44 ./internal ./tag ./cmd/generate-fix ./cmd/generate-fix/internal
	cd fix42 && go build -v `go list ./...`
	cd fix44 && go build -v `go list ./...`

build_all_win: 
	go build -v . ./config ./datadictionary ./enum ./field ./fix42 ./fix44 ./internal ./tag ./cmd/generate-fix ./cmd/generate-fix/internal
	cd fix42; go build -v `go list ./...`
	cd fix44; go build -v `go list ./...`

# ---------------------------------------------------------------
# Targets related to running acceptance tests -

build-test-srv:
	cd _test; go build -o echo_server ./test-server/
fix40:
	cd _test; ./runat.sh $@.cfg 5001 "definitions/server/$@/*.def"
fix41:
	cd _test; ./runat.sh $@.cfg 5002 "definitions/server/$@/*.def"
fix42:
	cd _test; ./runat.sh $@.cfg 5003 "definitions/server/$@/*.def"
fix43:
	cd _test; ./runat.sh $@.cfg 5004 "definitions/server/$@/*.def"
fix44:
	cd _test; ./runat.sh $@.cfg 5005 "definitions/server/$@/*.def"
fix50:
	cd _test; ./runat.sh $@.cfg 5006 "definitions/server/$@/*.def"
fix50sp1:
	cd _test; ./runat.sh $@.cfg 5007 "definitions/server/$@/*.def"
fix50sp2:
	cd _test; ./runat.sh $@.cfg 5008 "definitions/server/$@/*.def"

#ACCEPT_SUITE=fix40 fix41 fix42 fix43 fix44 fix50 fix50sp1 fix50sp2 
ACCEPT_SUITE=fix42 fix44
accept: $(ACCEPT_SUITE)

.PHONY: test $(ACCEPT_SUITE)
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# These targets are specific to the Github CI Runner -

build-src:
	go build -v `go list ./...`

build: build-src build-test-srv

test-ci:
	go test -v -cover . ./datadictionary ./internal

generate-ci: clean
	mkdir -p gen; cd gen; go run ../cmd/generate-fix/generate-fix.go -pkg-root=github.com/cryptogarageinc/quickfix-go/gen ../spec/$(shell echo $(FIX_TEST) | tr '[:lower:]' '[:upper:]').xml; 

# ---------------------------------------------------------------
