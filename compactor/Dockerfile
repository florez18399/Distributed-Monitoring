FROM maven:3.8.6-openjdk-11 AS builder

WORKDIR /build
RUN echo '<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd"> \
  <modelVersion>4.0.0</modelVersion> \
  <groupId>org.example</groupId> \
  <artifactId>hive-deps-downloader</artifactId> \
  <version>1.0.0</version> \
  <dependencies> \
    <dependency> \
        <groupId>org.apache.hive</groupId> \
        <artifactId>hive-exec</artifactId> \
        <version>3.1.3</version> \
        <exclusions> \
            <exclusion><groupId>org.apache.logging.log4j</groupId><artifactId>*</artifactId></exclusion> \
            <exclusion><groupId>org.slf4j</groupId><artifactId>*</artifactId></exclusion> \
        </exclusions> \
    </dependency> \
    <dependency> \
        <groupId>org.apache.hive</groupId> \
        <artifactId>hive-metastore</artifactId> \
        <version>3.1.3</version> \
    </dependency> \
    <dependency> \
        <groupId>org.postgresql</groupId> \
        <artifactId>postgresql</artifactId> \
        <version>42.7.3</version> \
    </dependency> \
  </dependencies> \
</project>' > pom.xml

RUN mvn dependency:copy-dependencies -DoutputDirectory=/build/hive-libs

# ------------------------------------------------------------------
# Imagen PySpark + Cron para compactación diaria
# ------------------------------------------------------------------
FROM ubuntu:22.04

ARG SPARK_VERSION=3.2.1
ARG HADOOP_PROFILE=3.2
ARG SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz"
ARG SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV HADOOP_CONF_DIR=/opt/hadoop-config

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    cron \
    python3 \
    python3-pip \
    openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fSL ${SPARK_URL} -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

RUN curl -fSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar -o ${SPARK_HOME}/jars/spark-sql-kafka-0-10.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar -o ${SPARK_HOME}/jars/kafka-clients.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar -o ${SPARK_HOME}/jars/spark-token-provider-kafka-0-10.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.6.2/commons-pool2-2.6.2.jar -o ${SPARK_HOME}/jars/commons-pool2.jar

COPY --from=builder /build/hive-libs/* /opt/spark/hive-3.1-libs/

RUN pip3 install pyspark==${SPARK_VERSION}

WORKDIR /app
COPY compactor.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
RUN mkdir -p ${HADOOP_CONF_DIR}

# Cron: compactación cada 3 horas (las ultimas 3 horas, excluyendo la hora actual)
RUN echo "0 */3 * * * SPARK_HOME=/opt/spark PATH=/opt/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 spark-submit --master 'local[*]' /app/compactor.py --recent 3 >> /var/log/compactor.log 2>&1" > /etc/cron.d/compactor \
    && chmod 0644 /etc/cron.d/compactor \
    && crontab /etc/cron.d/compactor

CMD ["/app/entrypoint.sh"]
