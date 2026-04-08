.PHONY: build generate test test-race test-integration lint coverage coverage-html clean docker-build

BINARY=kubemq-keda-scaler
VERSION=1.0.0

build:
	CGO_ENABLED=0 go build -ldflags="-s -w -X main.Version=$(VERSION)" -o $(BINARY) .

generate:
	protoc --go_out=. --go_opt=module=github.com/kubemq-io/kubemq-keda \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/kubemq-io/kubemq-keda \
	       proto/externalscaler/externalscaler.proto

test:
	go test -timeout 30s ./...

test-race:
	go test -race -timeout 30s ./...

test-integration:
	go test -tags integration -race -timeout 5m ./...

lint:
	golangci-lint run ./...

coverage:
	go test -coverprofile=coverage.out -coverpkg=./... ./...
	go tool cover -func=coverage.out

coverage-html:
	go test -coverprofile=coverage.out -coverpkg=./... ./...
	go tool cover -html=coverage.out -o coverage.html

clean:
	rm -f $(BINARY) coverage.out coverage.html

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t kubemq/kubemq-keda-scaler:$(VERSION) .
	docker tag kubemq/kubemq-keda-scaler:$(VERSION) kubemq/kubemq-keda-scaler:latest
