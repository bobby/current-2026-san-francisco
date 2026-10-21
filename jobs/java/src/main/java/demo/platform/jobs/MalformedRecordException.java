package demo.platform.jobs;

/** A record that couldn't be parsed, with its exact position in Kafka. */
public final class MalformedRecordException extends RuntimeException {

    public MalformedRecordException(String topic, int partition, long offset, Throwable cause) {
        super("Malformed record in %s partition %d at offset %d: %s"
                .formatted(topic, partition, offset, cause.getMessage()), cause);
    }
}
