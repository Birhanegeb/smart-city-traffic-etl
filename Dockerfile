FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    openjdk-17-jdk \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.0.1 \
    pyspark \
    psycopg2-binary