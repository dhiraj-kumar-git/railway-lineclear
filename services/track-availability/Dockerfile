FROM python:3.11-slim

RUN pip install --no-cache-dir "uv==0.11.*"

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY lineclear ./lineclear
COPY app ./app
COPY sql ./sql

RUN useradd --uid 10001 --no-create-home appuser
USER 10001

EXPOSE 50051 9100
CMD ["/app/.venv/bin/python", "-m", "app.server"]