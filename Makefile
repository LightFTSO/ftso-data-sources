project_name = creativerotarydie
image_name = creativerotarydie:latest
SRC_DIR=./cmd

build-dev:
	go build -o build/app $(SRC_DIR)/app.go

run: build-dev
	./build/app

watch:
	ENV=development reflex -s -r '\.go$$' make run

watch-prod:
	ENV=production reflex -s -r '\.go$$' make run

run-local:
	ENV=development go run $(SRC_DIR)/app.go

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
