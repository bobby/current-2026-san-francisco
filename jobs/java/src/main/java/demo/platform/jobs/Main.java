package demo.platform.jobs;

/**
 * Entry point for every Java job on the platform. The job is chosen with
 * {@code --job=<name>}; see {@link JobOptions} for the other flags.
 */
public final class Main {

    public static void main(String[] args) throws Exception {
        JobOptions options = JobOptions.parse(args);
        switch (options.job()) {
            case PaymentsEnrichment.NAME -> PaymentsEnrichment.run(options);
            case TradeEventsIngest.NAME -> TradeEventsIngest.run(options);
            case PositionsAggregator.NAME -> PositionsAggregator.run(options);
            default -> throw new IllegalArgumentException("Unknown job: " + options.job());
        }
    }

    private Main() {}
}
