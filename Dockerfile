FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir poetry

COPY --chown=airflow:root pyproject.toml poetry.lock /opt/airflow/
WORKDIR /opt/airflow

RUN poetry config virtualenvs.create false \
    && poetry install --no-root --only main

# --- crypto-market-rag dependencies ---------------------------------------
# Installed via plain pip (not poetry), since this Dockerfile's poetry
# install above is scoped to THIS project's pyproject.toml/poetry.lock only.
# Versions here match crypto-market-rag/pyproject.toml.
RUN pip install --no-cache-dir \
    "psycopg[binary]>=3.1,<4.0" \
    "pgvector>=0.2,<0.3" \
    "ollama>=0.2,<0.3" \
    "feedparser>=6.0,<7.0" \
    "requests>=2.32,<3.0" \
    "beautifulsoup4>=4.12,<5.0" \
    "tiktoken>=0.7,<0.8" \
    "pydantic-settings>=2.2,<3.0" \
    "structlog>=24.1,<25.0" \
    "typer>=0.12,<0.13" \
    "rich>=13.7,<14.0"

COPY --chown=airflow:root . /opt/airflow/
