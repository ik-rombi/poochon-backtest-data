# Stage 1: install Python deps with uv
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

WORKDIR /app

COPY pyproject.toml uv.lock .python-version README.md ./
COPY src/ src/

RUN uv sync --frozen --no-dev

# Stage 2: runtime
FROM python:3.14-slim-bookworm

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY src/ src/
COPY pyproject.toml README.md ./

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"
ENV PORT=8080

EXPOSE 8080

CMD ["python", "-m", "poochon_backtest_data.cli", "api"]
