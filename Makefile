.ONESHELL:
SHELL = /bin/sh
.SHELLFLAGS = -ec

BASE_PACKAGE := github.com/rxxx979/sqlx

.PHONY: tooling has-changes lint fmt vuln-check test test-race bench bench-pg update-dependencies

tooling:
	go install honnef.co/go/tools/cmd/staticcheck@2026.2.2
	go install golang.org/x/vuln/cmd/govulncheck@latest
	go install golang.org/x/tools/cmd/goimports@latest

has-changes:
	git diff --exit-code --quiet HEAD --

lint:
	go vet ./...
	staticcheck -checks=all ./...

fmt:
	go list -f '{{.Dir}}' ./... | xargs -I {} goimports -local $(BASE_PACKAGE) -w {}

vuln-check:
	govulncheck ./...

test:
	go test -count=1 ./...

test-race:
	go test -race -count=1 ./...

bench:
	go test -run '^$$' -bench . -benchmem ./...

bench-pg:
	@if [ -z "$$SQLX_POSTGRES_DSN" ]; then \
		echo "SQLX_POSTGRES_DSN is required, e.g. postgres://user:pass@localhost:5432/db?sslmode=disable"; \
		exit 1; \
	fi
	go test -run '^$$' -bench '^BenchmarkPG' -benchmem ./...

update-dependencies:
	go get -u -t -v ./...
	go mod tidy
