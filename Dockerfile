FROM ghcr.io/astral-sh/uv:0.13.5 AS uv
FROM python:3.13.17-slim
ARG BUILD_REVISION=unknown
ARG BUILD_DIRTY=unknown
ARG INSTALL_ELT=false
LABEL org.opencontainers.image.revision=$BUILD_REVISION

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PREFECT_HOME=/home/app/.prefect \
    GREEN_TAXI_BUILD_REVISION=$BUILD_REVISION \
    GREEN_TAXI_BUILD_DIRTY=$BUILD_DIRTY \
    DO_NOT_TRACK=1
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ ./src/
RUN case "$INSTALL_ELT" in \
      true) uv sync --locked --no-dev --no-editable --extra elt ;; \
      false) uv sync --locked --no-dev --no-editable ;; \
      *) echo 'INSTALL_ELT must be true or false' >&2; exit 2 ;; \
    esac \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home app \
    && mkdir -p /data /home/app/.prefect /home/app/ui \
    && chown -R app:app /data /home/app
USER app
ENTRYPOINT ["uv", "run", "--no-sync"]
CMD ["python", "-m", "green_taxi", "--run", "--data-dir", "/data"]
