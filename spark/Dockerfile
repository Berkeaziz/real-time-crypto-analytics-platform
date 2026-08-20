FROM python:3.12-slim

ARG INSTALL_DEV=false


WORKDIR /app
RUN apt-get update && apt-get install -y \
    curl \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"


COPY requirements.txt requirements-dev.txt ./

RUN pip install --no-cache-dir -r requirements.txt \
    && if [ "$INSTALL_DEV" = "true" ]; then \
        pip install --no-cache-dir -r requirements-dev.txt; \
    fi
RUN curl -o /tmp/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

COPY . .

CMD ["tail", "-f", "/dev/null"]