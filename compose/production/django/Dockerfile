# define an alias for the specific python version used in this file.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS python-build-stage

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

ARG APP_HOME=/app

WORKDIR ${APP_HOME}

# Install apt packages
RUN apt-get update && apt-get install --no-install-recommends -y \
  # dependencies for building Python packages \
  build-essential \
  # psycopg dependencies \
  libpq-dev \
  # coincurve (secp256k1 bindings) build dependencies \
  pkg-config \
  libsecp256k1-dev

# Requirements are installed here to ensure they will be cached.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --group production --no-group dev
COPY . ${APP_HOME}

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --group production --no-group dev

FROM node:22-bookworm-slim AS frontend-build-stage

WORKDIR /frontend

COPY pay-fronted/package.json pay-fronted/pnpm-lock.yaml pay-fronted/.npmrc ./

RUN corepack enable && pnpm approve-builds @tailwindcss/oxide esbuild && pnpm install --no-frozen-lockfile

COPY pay-fronted/ ./

RUN pnpm build

# Python 'run' stage
FROM python:3.13-slim-bookworm AS python-run-stage

ARG APP_HOME=/app

WORKDIR ${APP_HOME}

RUN addgroup --system django \
  && adduser --system --ingroup django django

# Install required system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
  # psycopg runtime dependency \
  libpq5 \
  # Translations dependencies \
  gettext \
  # entrypoint \
  wait-for-it \
  # cleaning up unused files \
  && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
  && rm -rf /var/lib/apt/lists/*

# 复制项目启动脚本
COPY --chown=django:django ./compose/production/django/entrypoint /entrypoint
COPY --chown=django:django ./compose/production/django/start /start
COPY --chown=django:django ./compose/production/django/celery/worker/start /start-worker
COPY --chown=django:django ./compose/production/django/celery/beat/start /start-beat
RUN set -eux; \
    sed -i 's/\r$//g' /entrypoint /start /start-worker /start-beat; \
    chmod +x /entrypoint /start /start-worker /start-beat

# Copy the application from the builder
COPY --from=python-build-stage --chown=django:django ${APP_HOME} ${APP_HOME}
COPY --from=frontend-build-stage --chown=django:django /frontend/dist /app/pay-fronted/dist

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH" \
    DJANGO_SETTINGS_MODULE=config.settings.production

RUN chmod +x ./compose/production/django/build-assets.sh && \
    ./compose/production/django/build-assets.sh

USER django

ENTRYPOINT ["/entrypoint"]
