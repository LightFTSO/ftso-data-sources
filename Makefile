project_name := ftso-data-sources
image_name := ftso-data-sources:latest
SRC_DIR=.
PROTO_ROOT = datasource/cryptocurrency/mexc/proto
OUT_DIR = datasource/cryptocurrency/mexc/pb
MOD = $(shell go list -m -f '{{.Path}}')
PROTO_FILES = $(shell find $(PROTO_ROOT) -maxdepth 1 -name '*.proto')

# mappings (no go_package)
MAPS = $(foreach f,$(PROTO_FILES),\
  --go_opt=M$(patsubst $(PROTO_ROOT)/%,%,$(f))=$(MOD)/$(OUT_DIR) \
)

.PHONY: proto-mexc

build-dev:
	go build -o build/main $(SRC_DIR)/main.go

build-dev-gcflags:
	go build -gcflags=all="-N -l" -o build/main $(SRC_DIR)/main.go

run: build-dev
	./build/main

watch:
	ENV=development air -c air.conf

watch-prod:
	ENV=production air -c air.conf

run-local:
	ENV=development go run $(SRC_DIR)/main.go

requirements:
	go mod tidy

clean-packages:
	go clean -modcache

up: 
	make up-silent
	make shell

build:
	docker compose build

build-no-cache:
	docker compose build --no-cache

up-silent:
	make delete-container-if-exist
	docker compose up -d

up-silent-prefork:
	make delete-container-if-exist
	docker compose up -d

delete-container-if-exist:
	docker stop $(project_name) || true && docker rm $(project_name) || true

shell:
	docker exec -it $(project_name) /bin/sh

stop:
	docker compose down

start:
	docker compose up -d

proto-mexc:
	mkdir -p $(OUT_DIR)
	echo $(MAPS)
	protoc -I $(PROTO_ROOT) \
	  --go_out=$(OUT_DIR) --go_opt=paths=source_relative \
	  $(MAPS) \
	  $(PROTO_FILES)