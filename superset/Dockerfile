FROM apache/superset:latest

USER root

ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN uv pip install --no-cache-dir psycopg2-binary && \
    /app/.venv/bin/python -c "import psycopg2"

USER superset