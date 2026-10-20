# Flink image for the demo platform.
#
# Flink 2.1 is the newest release supported by both the Kafka connector
# (5.0.0-2.1) and the Iceberg Flink runtime (iceberg-flink-runtime-2.1:1.11.0).
# Connector and Iceberg jars are added alongside the jobs in #2.
FROM flink:2.1.2-java17

# S3 filesystem for checkpoints and savepoints (RustFS locally, S3 on AWS).
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
 && ln -s /opt/flink/opt/flink-s3-fs-presto-2.1.2.jar /opt/flink/plugins/s3-fs-presto/

# Platform-wide settings. Runtime-specific settings (S3 endpoint and
# credentials, memory sizes, slots) come from FLINK_PROPERTIES in Compose.
COPY --chown=flink:flink conf/config.yaml /opt/flink/conf/config.yaml
COPY --chown=flink:flink conf/log4j-console.properties /opt/flink/conf/log4j-console.properties
COPY --chown=flink:flink conf/log-template.json /opt/flink/conf/log-template.json
