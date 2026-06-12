# CupidMQ — make from repo root: make help | make build | make publish

ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
MASTER := $(ROOT)master
CLIENT := $(ROOT)python-client
DASHBOARD := $(ROOT)dashboard
DIST := $(ROOT)dist

DOCKER_SCALE_PRODUCER ?= 8
DOCKER_SCALE_CONSUMER_RUST ?= 8
DOCKER_SCALE_CONSUMER_PYTHON ?= 8
DOCKER_COMPOSE := docker compose -f $(ROOT)test-environments/integration/docker-compose.yml --env-file $(ROOT)test-environments/integration/.env

.PHONY: help build run producer-run consumer-run dashboard test test-rust test-python stress sim publish publish-packages check-release docker-master-up docker-master-down docker-up docker-down

help:
	@echo "CupidMQ"
	@echo "  make build          cargo build --release master + cupidmq-producer"
	@echo "  make run            start master (cupidmq.conf)"
	@echo "  make producer-run   example producer load"
	@echo "  make consumer-run   Python harness consumer"
	@echo "  make dashboard      Vite dev :5175"
	@echo "  make test-rust      cargo test in master/"
	@echo "  make test-python    uv sync + unittest in python-client/"
	@echo "  make stress         stress-tcp.ps1 (16p x 20c)"
	@echo "  make sim            sim-load.ps1"
	@echo "  make publish        release binaries -> dist/"
	@echo "  make publish-packages  wheel + .crate -> dist/"
	@echo "  make check-release TAG=v0.1.0  verify versions before tag"
	@echo "  make docker-master-up   master only (root docker-compose.yml)"
	@echo "  make docker-master-down stop master compose"
	@echo "  make docker-up          integration compose 8p + 8 rust + 8 py"
	@echo "  make docker-down        integration compose down -v"

build:
	cd $(MASTER) && cargo build --release --example cupidmq-producer

run:
	cd $(MASTER) && cargo run --release -- --config cupidmq.conf

producer-run:
	cd $(MASTER) && cargo run --release --example cupidmq-producer -- --rate 50 --duration-secs 30 --batch-mode

consumer-run:
	cd $(CLIENT) && uv sync && uv run python -m harness.consumer_cli

dashboard:
	cd $(DASHBOARD) && npm install && npm run dev

test-rust:
	cd $(MASTER) && cargo test

test-python:
	cd $(CLIENT) && uv sync && uv run python -m unittest discover -s tests -p "test_*.py" -v

stress: build
	powershell -ExecutionPolicy Bypass -File $(ROOT)scripts/stress-tcp.ps1

sim: build
	powershell -ExecutionPolicy Bypass -File $(ROOT)scripts/sim-load.ps1

publish: build
	powershell -ExecutionPolicy Bypass -File $(ROOT)scripts/publish.ps1

publish-packages:
	powershell -ExecutionPolicy Bypass -File $(ROOT)scripts/publish-packages.ps1

check-release:
ifndef TAG
	$(error set TAG=v0.1.0 — e.g. make check-release TAG=v0.1.0)
endif
	powershell -ExecutionPolicy Bypass -File $(ROOT)scripts/check-release-version.ps1 -Tag $(TAG)

docker-master-up:
	docker compose up -d --build

docker-master-down:
	docker compose down

docker-up:
	$(DOCKER_COMPOSE) up -d --build \
		--scale producer=$(DOCKER_SCALE_PRODUCER) \
		--scale consumer-rust=$(DOCKER_SCALE_CONSUMER_RUST) \
		--scale consumer-python=$(DOCKER_SCALE_CONSUMER_PYTHON)

docker-down:
	$(DOCKER_COMPOSE) down -v
