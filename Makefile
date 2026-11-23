.DEFAULT_GOAL := help
.PHONY: help init config require-env run up stop gen db build logs down check-goose check-db-env goose_status goose_up goose_down goose_create goose_reset

# Compose parses .env itself, including quotes and environment overrides.
# Read it in Make only for host-side database maintenance commands.
ifneq ($(filter goose_status goose_up goose_down goose_reset check-db-env,$(MAKECMDGOALS)),)
-include .env
endif

DB_HOST ?= 127.0.0.1
DB_HOST_PORT ?= 5433
DB_PORT ?= $(DB_HOST_PORT)
DB_STRING = "host=$(DB_HOST) port=$(DB_PORT) user=$(DB_USER) password=$(DB_PASS) dbname=$(DB_NAME) sslmode=disable"
DB_DRIVER = postgres
MIG_DIR = migrations
GOOSE ?= goose

## help: Показать список команд (также команда по умолчанию)
help:
	@awk '/^## / { sub(/^## /, ""); pos = index($$0, ":"); printf "  %-16s %s\n", substr($$0, 1, pos - 1), substr($$0, pos + 2) }' $(firstword $(MAKEFILE_LIST))

## init: Создать .env с новыми секретами; существующий файл сохранить
init:
	@bash scripts/init-env.sh

require-env:
	@test -f .env || { printf '%s\n' 'Нет .env: сначала выполните make init.' >&2; exit 1; }

## config: Проверить Compose-конфигурацию без вывода секретов
config: require-env
	@docker compose config --quiet

## run: Собрать, запустить, дождаться готовности и показать логи
run: up
	@$(MAKE) --no-print-directory logs

## up: Собрать и запустить основной стек в фоне
up: config
	docker compose up -d --build --wait --wait-timeout 120

## stop: Остановить стек, сохранив контейнеры и данные
stop: require-env
	docker compose --profile redis stop

## gen: Сгенерировать Go-код протоколов (нужны Go/protoc/plugins)
gen:
	@command -v go >/dev/null || { printf '%s\n' 'Для генерации нужен Go.' >&2; exit 1; }
	@PATH="$$(go env GOPATH)/bin:$$PATH" bash scripts/gen-proto.sh

## db: Запустить только PostgreSQL
db: config
	docker compose up -d --wait --wait-timeout 120 db

.PHONY: redis-up redis-stop
## redis-up: Запустить опциональный Redis для уведомлений E2EE
redis-up: config
	docker compose --profile redis up -d --wait --wait-timeout 60 redis

## redis-stop: Остановить только Redis, сохранив данные
redis-stop: require-env
	docker compose --profile redis stop redis

## build: Собрать Docker-образ приложения
build: config
	docker compose build

## logs: Следить за логами backend; Ctrl+C оставляет стек работающим
logs: require-env
	docker compose logs -f server

## down: Удалить контейнеры и сеть; тома с данными сохранить
down: require-env
	docker compose --profile redis down

check-goose:
	@command -v "$(GOOSE)" >/dev/null || { printf '%s\n' 'Нужен Goose CLI в PATH или GOOSE=/путь/к/goose.' >&2; exit 1; }

check-db-env: require-env
	@test '$(if $(strip $(DB_USER)),set,missing)' = set && test '$(if $(strip $(DB_PASS)),set,missing)' = set && test '$(if $(strip $(DB_NAME)),set,missing)' = set || { printf '%s\n' 'Заполните DB_USER, DB_PASS и DB_NAME в .env.' >&2; exit 1; }

## goose_status: Показать статус миграций через host Goose CLI
goose_status: check-db-env check-goose
	@"$(GOOSE)" -dir $(MIG_DIR) $(DB_DRIVER) $(DB_STRING) status

## goose_up: Применить миграции через host Goose CLI
goose_up: check-db-env check-goose
	@"$(GOOSE)" -dir $(MIG_DIR) $(DB_DRIVER) $(DB_STRING) up

## goose_down: Откатить последнюю миграцию; может удалить данные
goose_down: check-db-env check-goose
	@"$(GOOSE)" -dir $(MIG_DIR) $(DB_DRIVER) $(DB_STRING) down

## goose_create: Создать миграцию: make goose_create name=add_users_table
goose_create: check-goose
	@test -n "$(name)" || { printf '%s\n' 'Укажите name=имя_миграции.' >&2; exit 1; }
	@"$(GOOSE)" -dir $(MIG_DIR) create "$(name)" sql

## goose_reset: Откатить ВСЕ миграции; удаляет данные
goose_reset: check-db-env check-goose
	@"$(GOOSE)" -dir $(MIG_DIR) $(DB_DRIVER) $(DB_STRING) reset

.PHONY: monitor-up monitor-stop load-init load-up load-seed load-run load-check load-observe load-info load-stop load-down load-mixed load-mixed-race
## monitor-up: Запустить Prometheus/Grafana/exporter вместе с основным стеком
monitor-up: require-env
	docker compose -f compose.yaml -f compose.monitoring.yaml up -d --build --wait --wait-timeout 180

## monitor-stop: Остановить только мониторинг, сохранив его историю
monitor-stop: require-env
	docker compose -f compose.yaml -f compose.monitoring.yaml stop grafana prometheus postgres-exporter

## load-init: Создать отдельную конфигурацию тестового стенда в out/load
load-init:
	python3 scripts/load/lab.py init

## load-up: Собрать отдельный стенд нагрузки со звонками и мониторингом
load-up:
	python3 scripts/load/lab.py up

## load-seed: Подготовить тестовые пары аккаунтов: LOAD_PAIRS=10
load-seed:
	python3 scripts/load/lab.py seed --pairs $(or $(LOAD_PAIRS),10)

## load-run: Запустить нагрузочный профиль: LOAD_PROFILE=smoke/baseline/stress/spike/soak
load-run:
	python3 scripts/load/lab.py run --profile $(or $(LOAD_PROFILE),smoke)

## load-mixed: Короткое сравнение WS/E2EE/PG (MIXED_SECONDS=8, MIXED_WORKERS=32)
load-mixed:
	python3 scripts/load/mixed.py --seconds $(or $(MIXED_SECONDS),8) --workers $(or $(MIXED_WORKERS),32) --admission $(or $(MIXED_ADMISSION),enabled)

## load-mixed-race: Короткая проверка mixed-сценария и RPC-регрессий с race detector
load-mixed-race:
	python3 scripts/load/mixed.py --race

## load-check: Проверить реальные метрики, dashboard и работу без Prometheus
load-check:
	python3 scripts/load/lab.py verify

## load-observe: Собирать ресурсы во время ручной проверки звонков (LOAD_SECONDS=900)
load-observe:
	python3 scripts/load/lab.py observe --seconds $(or $(LOAD_SECONDS),900)

## load-info: Показать адреса отдельного тестового стенда
load-info:
	python3 scripts/load/lab.py info

## load-stop: Остановить тестовый стенд, сохранив данные и графики
load-stop:
	python3 scripts/load/lab.py stop

## load-down: Удалить тестовые контейнеры/сеть, сохранив тома и отчёты
load-down:
	python3 scripts/load/lab.py down
