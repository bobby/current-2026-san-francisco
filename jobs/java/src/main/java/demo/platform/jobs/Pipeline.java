package demo.platform.jobs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Plumbing shared by the Java jobs. Kafka and Iceberg are reached through the
 * Table API, using the connectors and the {@code lake} catalog the cluster
 * provides, so the jobs bundle no connector code. Only the processing in the
 * middle is DataStream code.
 */
final class Pipeline {

    final String name;
    final JobOptions options;
    final StreamExecutionEnvironment env;
    final StreamTableEnvironment tableEnv;
    private final StreamStatementSet inserts;

    Pipeline(String name, JobOptions options) {
        this.name = name;
        this.options = options;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment(jobConfiguration(options));
        this.tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance()
                .withConfiguration(catalogStoreConfiguration())
                .build());
        this.inserts = tableEnv.createStatementSet();
    }

    /** Job-level settings that override the cluster's: the state backend, if chosen. */
    private static Configuration jobConfiguration(JobOptions options) {
        Configuration config = new Configuration();
        if (options.stateBackend() != null) {
            config.set(StateBackendOptions.STATE_BACKEND, options.stateBackend());
        }
        return config;
    }

    /**
     * The cluster's catalog store settings ({@code table.catalog-store.*} in
     * flink/conf/config.yaml), which define the {@code lake} catalog. A job's
     * table environment doesn't pick these up on its own.
     */
    private static Configuration catalogStoreConfiguration() {
        Configuration cluster = GlobalConfiguration.loadConfiguration();
        Configuration catalogStore = new Configuration();
        cluster.toMap().forEach((key, value) -> {
            if (key.startsWith("table.catalog-store.")) {
                catalogStore.setString(key, value);
            }
        });
        return catalogStore;
    }

    /** Runs DDL (e.g. creating the job's Iceberg database and table) at submission time. */
    void sql(String statement) {
        tableEnv.executeSql(statement);
    }

    /**
     * Bounds an Iceberg table's metadata. Each commit (one per checkpoint)
     * writes a new metadata file, and Iceberg keeps every one unless told to
     * delete old versions. Applied on every deploy, so existing tables get it
     * too.
     */
    void retainMetadata(String table) {
        sql("ALTER TABLE " + table + " SET ('write.metadata.delete-after-commit.enabled' = 'true', "
                + "'write.metadata.previous-versions-max' = '10')");
    }

    /**
     * Reads a Kafka topic as raw strings with their topic, partition and
     * offset, so malformed records can be reported precisely. Offsets are
     * committed to the job's consumer group on each checkpoint; a job deployed
     * without a savepoint resumes from them.
     */
    DataStream<Row> kafkaSource(String topic) {
        String table = tableName(topic) + "_raw";
        sql("""
                CREATE TEMPORARY TABLE %s (
                  `topic` STRING METADATA VIRTUAL,
                  `partition` INT METADATA VIRTUAL,
                  `offset` BIGINT METADATA VIRTUAL,
                  `value` STRING
                ) WITH (
                  'connector' = 'kafka',
                  'topic' = '%s',
                  'properties.bootstrap.servers' = '%s',
                  'properties.group.id' = '%s',
                  'properties.auto.offset.reset' = 'earliest',
                  'scan.startup.mode' = 'group-offsets',
                  'value.format' = 'raw'
                )""".formatted(table, topic, options.bootstrapServers(), name));
        return tableEnv.toDataStream(tableEnv.from(table));
    }

    /**
     * Parses raw Kafka records. Malformed ones fail the job or go to
     * {@code <topic>.dlq}, depending on {@code --on-deser-error}.
     */
    DataStream<Row> parse(String topic, DataStream<Row> raw, RecordParser parser, TypeInformation<Row> type) {
        SingleOutputStreamOperator<Row> parsed = raw
                .process(new ParseOrDivert(parser, options.onDeserError()))
                .returns(type)
                .name("parse " + topic)
                .uid("parse-" + topic);

        if (options.onDeserError() == JobOptions.DeserErrorMode.DLQ) {
            String dlqTable = tableName(topic) + "_dlq";
            sql("""
                    CREATE TEMPORARY TABLE %s (
                      source_topic STRING,
                      source_partition INT,
                      source_offset BIGINT,
                      error STRING,
                      payload STRING,
                      diverted_at TIMESTAMP_LTZ(3)
                    ) WITH (
                      'connector' = 'kafka',
                      'topic' = '%s.dlq',
                      'properties.bootstrap.servers' = '%s',
                      'value.format' = 'json',
                      'sink.delivery-guarantee' = 'at-least-once'
                    )""".formatted(dlqTable, topic, options.bootstrapServers()));
            tableEnv.createTemporaryView(dlqTable + "_view", parsed.getSideOutput(ParseOrDivert.DLQ));
            insert("INSERT INTO %s SELECT source_topic, source_partition, source_offset, error, payload, "
                    .formatted(dlqTable)
                    + "TO_TIMESTAMP_LTZ(diverted_at_ms, 3) FROM " + dlqTable + "_view");
        }
        return parsed;
    }

    /** Registers a stream as a temporary view for use in {@link #insert}. */
    void view(String name, DataStream<Row> stream) {
        tableEnv.createTemporaryView(name, stream);
    }

    /** Adds an INSERT (typically into a {@code lake.*} Iceberg table) to the job. */
    void insert(String statement) {
        inserts.addInsertSql(statement);
    }

    void execute() throws Exception {
        inserts.attachAsDataStream();
        env.execute(name);
    }

    private static String tableName(String topic) {
        return topic.replace('-', '_');
    }
}
