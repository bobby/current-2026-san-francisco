# Flink image for the demo platform.
#
# Flink 2.1 is the newest release supported by both the Kafka connector
# (5.0.0-2.1) and the Iceberg Flink runtime (iceberg-flink-runtime-2.1:1.11.0).
FROM flink:2.1.2-java17

# S3 filesystem for checkpoints and savepoints (RustFS locally, S3 on AWS).
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
 && ln -s /opt/flink/opt/flink-s3-fs-presto-2.1.2.jar /opt/flink/plugins/s3-fs-presto/

# Connectors shared by every job, on the cluster classpath so the SQL Gateway
# and the Java jobs (which declare them as `provided`) use the same versions.
# Iceberg's Flink catalog still needs Hadoop's Configuration class, hence the
# Hadoop client jars. Each download is checked against Maven Central's SHA-1.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eu; cd /opt/flink/lib; \
    for path in \
      org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar \
      org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar \
      org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar \
      org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar \
      org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar; \
    do \
      jar="${path##*/}"; \
      curl -fsSL -o "$jar" "$MAVEN/$path"; \
      echo "$(curl -fsSL "$MAVEN/$path.sha1" | cut -c1-40)  $jar" | sha1sum -c -; \
    done; \
    chown flink:flink /opt/flink/lib/*.jar

# Platform-wide settings. Runtime-specific settings (S3 endpoint and
# credentials, memory sizes, slots) come from FLINK_PROPERTIES in Compose, and
# the Iceberg catalog definition is mounted per runtime at /opt/flink/catalogs.
COPY --chown=flink:flink conf/config.yaml /opt/flink/conf/config.yaml
COPY --chown=flink:flink conf/log4j-console.properties /opt/flink/conf/log4j-console.properties
COPY --chown=flink:flink conf/log-template.json /opt/flink/conf/log-template.json
