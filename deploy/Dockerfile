# Strata in an image: the web app built with Node, Python with uv, run as a non-root user.
#   docker build -f deploy/Dockerfile -t strata .
#   docker run --rm -v strata-data:/data strata profile peps.python.org --html /data/peps.html
#   docker run --rm -p 8040:8040 --env-file .env strata console --host 0.0.0.0

FROM node:22-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web ./
RUN npx tsc -b && npx vite build --outDir /static

FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.10.0 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY packages ./packages
COPY --from=web /static ./packages/strata-console/src/strata_console/static
RUN uv sync --locked --no-dev --no-editable

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 strata && mkdir /data && chown strata /data
COPY --from=build /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH \
    STRATA_CACHE_DIR=/data/cache \
    STRATA_RAW_ROOT=/data/raw \
    STRATA_LAKE_DATA_PATH=/data/lake
USER strata
WORKDIR /data
# 8040: `strata console`; 50061 and 9461: `strata scheduler`'s gRPC port and its /metrics
EXPOSE 8040 50061 9461
ENTRYPOINT ["strata"]
CMD ["--help"]
