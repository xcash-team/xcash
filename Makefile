ENV_FILE ?= .env
DC = docker compose --env-file $(ENV_FILE) -f docker-compose.dev.yml

.PHONY: help dev-sync dev-up dev-up-pro dev-up-deps dev-up-chain dev-up-signer dev-down dev-logs dev-chain-logs dev-ps dev-web dev-worker dev-worker-stress dev-worker-scan dev-beat dev-manage dev-mm dev-migrate dev-clear-migrations dev-shell dev-test dev-local-init dev-local-bitcoin dev-signer-migrate dev-signer-check dev-signer-e2e dev-bootstrap

help:
	@echo "可用命令："
	@echo "  开发环境准备：cp .env.example .env 后按需改成开发值"
	@echo "  make dev-sync         同步本地开发依赖（uv dev group）"
	@echo "  make dev-up           前台运行 Django + Celery（开发模式）"
	@echo "  make dev-up-pro       生产级方式运行（gunicorn + 高并发 worker，适合压测）"
	@echo "  make dev-up-deps      仅启动 django-db/redis/signer-db/signer"
	@echo "  make dev-up-chain     启动 django-db/redis/signer-db/signer/anvil/bitcoin"
	@echo "  make dev-down         停止开发依赖容器"
	@echo "  make dev-logs         查看依赖容器日志"
	@echo "  make dev-chain-logs   查看本地区块链容器日志"
	@echo "  make dev-ps           查看依赖容器状态"
	@echo "  make dev-web          宿主机启动 Django"
	@echo "  make dev-worker       宿主机启动业务 Celery worker"
	@echo "  make dev-worker-stress 宿主机启动 stress Celery worker"
	@echo "  make dev-worker-scan  宿主机启动 scan Celery worker"
	@echo "  make dev-beat         宿主机启动 Celery beat"
	@echo "  make dev-manage ARGS='check'"
	@echo "  make dev-mm           宿主机执行 Django makemigrations"
	@echo "  make dev-migrate      宿主机执行 Django migrate"
	@echo "  make dev-clear-migrations 删除所有 app 的迁移文件（保留 __init__.py）"
	@echo "  make dev-shell        宿主机进入 Django shell_plus"
	@echo "  make dev-test         启动依赖后使用 Postgres/Redis 运行 Django 测试"
	@echo "  make dev-local-init   初始化本地联调链配置（anvil/regtest）"
	@echo "  make dev-local-bitcoin 准备 regtest 钱包、预挖区块并导入 BTC 地址"
	@echo "  make dev-up-signer    启动独立 signer 及其 PostgreSQL"
	@echo "  make dev-signer-migrate 执行独立 signer 数据库迁移"
	@echo "  make dev-signer-check 执行 signer 服务检查"
	@echo "  make dev-signer-e2e   启动主依赖和 signer，并串行执行迁移、检查"
	@echo "  make dev-bootstrap    从空数据卷初始化 signer、主库和本地联调链"

dev-sync:
	uv sync --group dev

dev-up:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-up.sh

dev-up-pro:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-up-pro.sh

dev-up-deps:
	$(DC) up -d django-db redis signer-db signer

dev-up-chain:
	$(DC) up -d django-db redis signer-db signer anvil bitcoin

dev-up-signer:
	$(DC) up -d redis signer-db signer

dev-signer-migrate:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-signer-manage.sh migrate

dev-down:
	$(DC) down

dev-logs:
	$(DC) logs -f django-db redis signer-db signer flower

dev-chain-logs:
	$(DC) logs -f anvil bitcoin

dev-ps:
	$(DC) ps

dev-web:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-web.sh

dev-worker:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-worker.sh

dev-worker-stress:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-worker-stress.sh

dev-worker-scan:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-worker-scan.sh

dev-beat:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-beat.sh

dev-manage:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-manage.sh $(ARGS)

dev-mm:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-manage.sh makemigrations

dev-migrate:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-manage.sh migrate

dev-clear-migrations:
	./scripts/dev-clear-migrations.sh

dev-shell:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-manage.sh shell_plus

dev-test:
	$(DC) up -d django-db redis signer-db signer
	# 复用已有测试库，避免非交互环境在 test_xcash 已存在时卡住确认提示。
	PYTHONPATH=xcash ./.venv/bin/python manage.py test --settings=config.settings.test --keepdb

dev-local-init:
	$(DC) up -d django-db redis signer-db signer anvil bitcoin
	# 本地链初始化与生产默认 init 分离，避免误写 Sepolia / mainnet 配置到开发库。
	ENV_FILE=$(ENV_FILE) ./scripts/dev-manage.sh init_local_chains

dev-local-bitcoin:
	$(DC) up -d bitcoin
	# regtest 钱包准备单独拆分，便于在创建新地址后重复导入 watch-only。
	ENV_FILE=$(ENV_FILE) ./scripts/dev-manage.sh prepare_local_bitcoin

dev-signer-check:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-manage.sh check_signer_service

dev-signer-e2e:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-signer-e2e.sh

dev-bootstrap:
	ENV_FILE=$(ENV_FILE) ./scripts/dev-bootstrap.sh
