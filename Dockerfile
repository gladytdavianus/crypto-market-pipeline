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

COPY --chown=airflow:root . /opt/airflow/
