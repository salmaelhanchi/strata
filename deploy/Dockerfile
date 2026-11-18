# Strata's CLI in an image: built with uv, run as a non-root user.
#   docker build -f deploy/Dockerfile -t strata .
#   docker run --rm -v strata-data:/data strata profile peps.python.org --html /data/peps.html

FROM python:3.12-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.10.0 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY packages ./packages
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
ENTRYPOINT ["strata"]
CMD ["--help"]
