package demo.platform.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * {@code trade-events-ingest} (tenant trading-data): validates trade events
 * and lands them in Iceberg.
 *
 * <p>Poison records (F2, docs/design.md §7.1): parsing is strict, so a
 * malformed record from upstream fails the job (restart loop) under
 * {@code --on-deser-error=fail}, or is diverted to {@code trade-events.dlq}
 * under {@code --on-deser-error=dlq}.
 */
final class TradeEventsIngest {

    static final String NAME = "trade-events-ingest";
    static final String TOPIC = "trade-events";

    private static final Set<String> SIDES = Set.of("BUY", "SELL");

    static final TypeInformation<Row> TRADE = Types.ROW_NAMED(
            new String[] {
                "trade_id", "account_id", "instrument", "side", "quantity", "price", "notional", "venue", "executed_at_ms"
            },
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.DOUBLE, Types.DOUBLE,
            Types.STRING, Types.LONG);

    static void run(JobOptions options) throws Exception {
        Pipeline p = new Pipeline(NAME, options);
        p.sql("CREATE DATABASE IF NOT EXISTS lake.trading_data");
        p.sql("""
                CREATE TABLE IF NOT EXISTS lake.trading_data.trade_events (
                  trade_id STRING,
                  account_id STRING,
                  instrument STRING,
                  side STRING,
                  quantity BIGINT,
                  price DOUBLE,
                  notional DOUBLE,
                  venue STRING,
                  executed_at TIMESTAMP_LTZ(6)
                )""");
        p.retainMetadata("lake.trading_data.trade_events");

        DataStream<Row> trades = p.parse(TOPIC, p.kafkaSource(TOPIC), TradeEventsIngest::parse, TRADE);

        p.view("trades", trades);
        p.insert("""
                INSERT INTO lake.trading_data.trade_events
                SELECT trade_id, account_id, instrument, side, quantity, price, notional, venue,
                       TO_TIMESTAMP_LTZ(executed_at_ms, 3)
                FROM trades""");
        p.execute();
    }

    static Row parse(String json) throws Exception {
        JsonNode trade = Json.read(json);
        String side = Json.text(trade, "side");
        if (!SIDES.contains(side)) {
            throw new IllegalArgumentException("field 'side' must be BUY or SELL, got '%s'".formatted(side));
        }
        long quantity = Json.integer(trade, "quantity");
        if (quantity <= 0) {
            throw new IllegalArgumentException("field 'quantity' must be positive, got " + quantity);
        }
        double price = Json.number(trade, "price");
        return Row.of(
                Json.text(trade, "trade_id"),
                Json.text(trade, "account_id"),
                Json.text(trade, "instrument"),
                side,
                quantity,
                price,
                quantity * price,
                Json.text(trade, "venue"),
                Json.integer(trade, "executed_at"));
    }

    private TradeEventsIngest() {}
}
