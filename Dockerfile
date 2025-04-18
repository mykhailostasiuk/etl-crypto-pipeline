FROM python:3.12.9-slim-bookworm AS builder

RUN apt-get update &&  \
    apt-get install -y openjdk-17-jdk wget tar && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.5-bin-hadoop3.tgz && \
    mv spark-3.5.5-bin-hadoop3 /opt/spark && \
    rm spark-3.5.5-bin-hadoop3.tgz

RUN useradd --create-home pipeline_user
WORKDIR /home/pipeline_user/crypto_pipeline

RUN chown -R pipeline_user:pipeline_user /home/pipeline_user/crypto_pipeline && chmod -R u+w /home/pipeline_user/crypto_pipeline

COPY pyproject.toml /home/pipeline_user/crypto_pipeline/
COPY scripts /home/pipeline_user/crypto_pipeline/scripts

USER pipeline_user

RUN pip wheel --no-cache-dir --no-deps --wheel-dir wheels .

FROM builder AS runner

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH

RUN pip install --no-cache /home/pipeline_user/crypto_pipeline/wheels/* && rm -rf /home/pipeline_user/crypto_pipeline/wheels