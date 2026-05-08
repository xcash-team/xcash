FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS python-build-stage

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

ARG APP_HOME=/app

WORKDIR ${APP_HOME}

RUN apt-get update && apt-get install --no-install-recommends -y \
  build-essential \
  libpq-dev \
  pkg-config \
  libsecp256k1-dev \
  && rm -rf /var/lib/apt/lists/*

# signer 生产镜像只解析 signer 依赖，避免把主应用的后台与任务依赖带进来。
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=signer/pyproject.toml,target=signer/pyproject.toml \
    --mount=type=bind,source=signer/uv.lock,target=signer/uv.lock \
    uv sync --project signer --locked --group production --no-group dev

COPY signer ${APP_HOME}/signer
COPY config/performance.py ${APP_HOME}/performance.py
COPY signer/compose/production/entrypoint /entrypoint
COPY signer/compose/production/start /start

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=signer/pyproject.toml,target=signer/pyproject.toml \
    --mount=type=bind,source=signer/uv.lock,target=signer/uv.lock \
    uv sync --project signer --locked --group production --no-group dev

FROM python:3.13-slim-bookworm AS python-run-stage

ARG APP_HOME=/app

WORKDIR ${APP_HOME}

RUN addgroup --system django \
  && adduser --system --ingroup django django

RUN apt-get update && apt-get install --no-install-recommends -y \
  libpq5 \
  wait-for-it \
  && rm -rf /var/lib/apt/lists/*

COPY --from=python-build-stage --chown=django:django /entrypoint /entrypoint
COPY --from=python-build-stage --chown=django:django /start /start
RUN set -eux; \
    sed -i 's/\r$//g' /entrypoint /start; \
    chmod +x /entrypoint /start

COPY --from=python-build-stage --chown=django:django ${APP_HOME} ${APP_HOME}

ENV PATH="/app/signer/.venv/bin:$PATH" \
    DJANGO_SETTINGS_MODULE=config.settings

USER django

ENTRYPOINT ["/entrypoint"]
