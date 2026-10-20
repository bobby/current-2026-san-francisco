package demo.platform.jobs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Parses raw Kafka rows ({@code topic, partition, offset, value}). A malformed
 * record either fails the job with a {@link MalformedRecordException} naming
 * its exact position, or is emitted to the {@link #DLQ} side output.
 */
final class ParseOrDivert extends ProcessFunction<Row, Row> {

    static final TypeInformation<Row> DLQ_TYPE = Types.ROW_NAMED(
            new String[] {"source_topic", "source_partition", "source_offset", "error", "payload", "diverted_at_ms"},
            Types.STRING, Types.INT, Types.LONG, Types.STRING, Types.STRING, Types.LONG);

    static final OutputTag<Row> DLQ = new OutputTag<>("dlq", DLQ_TYPE);

    private final RecordParser parser;
    private final JobOptions.DeserErrorMode mode;

    ParseOrDivert(RecordParser parser, JobOptions.DeserErrorMode mode) {
        this.parser = parser;
        this.mode = mode;
    }

    @Override
    public void processElement(Row raw, Context ctx, Collector<Row> out) {
        // Columns of the raw Kafka table in Pipeline#kafkaSource.
        String topic = (String) raw.getField(0);
        int partition = (Integer) raw.getField(1);
        long offset = (Long) raw.getField(2);
        String value = (String) raw.getField(3);

        Row parsed;
        try {
            parsed = parser.parse(value);
        } catch (Exception e) {
            if (mode == JobOptions.DeserErrorMode.FAIL) {
                throw new MalformedRecordException(topic, partition, offset, e);
            }
            ctx.output(DLQ, Row.of(topic, partition, offset, String.valueOf(e.getMessage()), value, System.currentTimeMillis()));
            return;
        }
        out.collect(parsed);
    }
}
