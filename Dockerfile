FROM ghcr.io/astral-sh/uv:0.13.5 AS uv
FROM python:3.13.17-slim

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PREFECT_HOME=/home/app/.prefect \
    DO_NOT_TRACK=1
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ ./src/
RUN uv sync --locked --no-dev --no-editable \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home app \
    && mkdir -p /data /home/app/.prefect \
    && chown -R app:app /data /home/app
USER app
ENTRYPOINT ["uv", "run", "--no-sync"]
CMD ["python", "-m", "green_taxi", "--run", "--data-dir", "/data"]
