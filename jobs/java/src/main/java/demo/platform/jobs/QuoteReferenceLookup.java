package demo.platform.jobs;

import java.util.Map;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * {@code reference_lookup(symbol)}, a SQL function for
 * {@code market-data-to-iceberg}: the asset class of a listed instrument, or
 * null for an unlisted one.
 *
 * <p>It stands in for a call to a reference-data service, with a fixed
 * latency per record ({@link #LATENCY_MS}). That gives the job a realistic,
 * hardware-independent capacity of about 200 records/s per subtask: plenty
 * for normal traffic (about 50/s), too little at parallelism 2 for a 10×
 * spike (F3, docs/design.md §7.1), and enough once rescaled to 3 or more.
 */
public final class QuoteReferenceLookup extends ScalarFunction {

    static final long LATENCY_MS = 5;

    private static final Map<String, String> ASSET_CLASS = Map.ofEntries(
            Map.entry("AAPL", "equity"), Map.entry("MSFT", "equity"), Map.entry("NVDA", "equity"),
            Map.entry("AMZN", "equity"), Map.entry("GOOGL", "equity"), Map.entry("JPM", "equity"),
            Map.entry("GS", "equity"), Map.entry("XOM", "equity"), Map.entry("TSLA", "equity"),
            Map.entry("META", "equity"), Map.entry("BRK.B", "equity"), Map.entry("V", "equity"),
            Map.entry("UNH", "equity"), Map.entry("JNJ", "equity"), Map.entry("WMT", "equity"));

    public String eval(String symbol) {
        try {
            Thread.sleep(LATENCY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return symbol == null ? null : ASSET_CLASS.get(symbol);
    }
}
