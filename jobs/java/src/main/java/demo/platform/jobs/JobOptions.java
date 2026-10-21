package demo.platform.jobs;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Command-line options shared by every Java job.
 *
 * <ul>
 *   <li>{@code --job=<name>}: which job to run (required).
 *   <li>{@code --fault=<name>}: enable a fault (repeatable, or comma-separated).
 *       Faults are how the CI/CD simulator deploys a "buggy version"
 *       (docs/design.md §7.1).
 *   <li>{@code --on-deser-error=fail|dlq}: what to do with a malformed input
 *       record: fail the job (the default), or divert it to {@code <topic>.dlq}.
 *   <li>{@code --bootstrap-servers=<hosts>}: Kafka bootstrap servers
 *       (default {@code kafka:9092}).
 *   <li>{@code --state-leak-bytes=<n>}: bytes retained per record by the
 *       {@code state-leak} fault (default 4096).
 * </ul>
 */
public record JobOptions(
        String job,
        Set<String> faults,
        DeserErrorMode onDeserError,
        String bootstrapServers,
        int stateLeakBytes)
        implements Serializable {

    public enum DeserErrorMode {
        FAIL,
        DLQ
    }

    public static final String FAULT_NPE_ON_NULL_FIELD = "npe-on-null-field";
    public static final String FAULT_STATE_LEAK = "state-leak";

    private static final Set<String> KNOWN_FAULTS = Set.of(FAULT_NPE_ON_NULL_FIELD, FAULT_STATE_LEAK);

    public boolean hasFault(String fault) {
        return faults.contains(fault);
    }

    public static JobOptions parse(String[] args) {
        String job = null;
        Set<String> faults = new LinkedHashSet<>();
        DeserErrorMode onDeserError = DeserErrorMode.FAIL;
        String bootstrapServers = "kafka:9092";
        int stateLeakBytes = 4096;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected argument: " + arg);
            }
            String key;
            String value;
            int eq = arg.indexOf('=');
            if (eq >= 0) {
                key = arg.substring(2, eq);
                value = arg.substring(eq + 1);
            } else if (i + 1 < args.length) {
                key = arg.substring(2);
                value = args[++i];
            } else {
                throw new IllegalArgumentException("Missing value for " + arg);
            }

            switch (key) {
                case "job" -> job = value;
                case "fault" -> {
                    for (String fault : value.split(",")) {
                        fault = fault.trim();
                        if (fault.isEmpty()) {
                            continue;
                        }
                        if (!KNOWN_FAULTS.contains(fault)) {
                            throw new IllegalArgumentException("Unknown fault: " + fault + " (known: " + KNOWN_FAULTS + ")");
                        }
                        faults.add(fault);
                    }
                }
                case "on-deser-error" -> onDeserError = DeserErrorMode.valueOf(value.toUpperCase());
                case "bootstrap-servers" -> bootstrapServers = value;
                case "state-leak-bytes" -> stateLeakBytes = Integer.parseInt(value);
                default -> throw new IllegalArgumentException("Unknown option: --" + key);
            }
        }

        if (job == null) {
            throw new IllegalArgumentException("--job is required");
        }
        return new JobOptions(job, Set.copyOf(faults), onDeserError, bootstrapServers, stateLeakBytes);
    }
}
