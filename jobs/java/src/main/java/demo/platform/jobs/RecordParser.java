package demo.platform.jobs;

import java.io.Serializable;
import org.apache.flink.types.Row;

/** Turns one raw JSON record into a typed row, throwing if it's malformed. */
@FunctionalInterface
interface RecordParser extends Serializable {
    Row parse(String json) throws Exception;
}
