package demo.platform.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * {@code payments-enrichment} (tenant payments-eng): enriches payments with a
 * USD amount, a normalized merchant category and a risk band.
 *
 * <p>Fault {@code npe-on-null-field} (F1, docs/design.md §7.1): the buggy
 * version assumes every merchant has a category. A few percent don't, so it
 * throws a NullPointerException within seconds and the job goes into a
 * restart loop.
 */
final class PaymentsEnrichment {

    static final String NAME = "payments-enrichment";
    static final String TOPIC = "payments";

    static final TypeInformation<Row> PAYMENT = Types.ROW_NAMED(
            new String[] {
                "payment_id", "account_id", "amount", "currency",
                "merchant_id", "merchant_name", "merchant_category", "channel", "created_at_ms"
            },
            Types.STRING, Types.STRING, Types.DOUBLE, Types.STRING,
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG);

    static final TypeInformation<Row> ENRICHED = Types.ROW_NAMED(
            new String[] {
                "payment_id", "account_id", "amount", "currency", "amount_usd",
                "merchant_id", "merchant_name", "merchant_category", "channel", "risk_band", "created_at_ms"
            },
            Types.STRING, Types.STRING, Types.DOUBLE, Types.STRING, Types.DOUBLE,
            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG);

    static void run(JobOptions options) throws Exception {
        Pipeline p = new Pipeline(NAME, options);
        p.sql("CREATE DATABASE IF NOT EXISTS lake.payments_eng");
        p.sql("""
                CREATE TABLE IF NOT EXISTS lake.payments_eng.payments_enriched (
                  payment_id STRING,
                  account_id STRING,
                  amount DOUBLE,
                  currency STRING,
                  amount_usd DOUBLE,
                  merchant_id STRING,
                  merchant_name STRING,
                  merchant_category STRING,
                  channel STRING,
                  risk_band STRING,
                  created_at TIMESTAMP_LTZ(6)
                )""");
        p.retainMetadata("lake.payments_eng.payments_enriched");

        DataStream<Row> payments = p.parse(TOPIC, p.kafkaSource(TOPIC), PaymentsEnrichment::parse, PAYMENT);
        DataStream<Row> enriched = payments
                .map(new Enrich(options.hasFault(JobOptions.FAULT_NPE_ON_NULL_FIELD)))
                .returns(ENRICHED)
                .name("enrich payments")
                .uid("enrich-payments");

        p.view("enriched", enriched);
        p.insert("""
                INSERT INTO lake.payments_eng.payments_enriched
                SELECT payment_id, account_id, amount, currency, amount_usd, merchant_id, merchant_name,
                       merchant_category, channel, risk_band, TO_TIMESTAMP_LTZ(created_at_ms, 3)
                FROM enriched""");
        p.execute();
    }

    static Row parse(String json) throws Exception {
        JsonNode payment = Json.read(json);
        JsonNode merchant = Json.object(payment, "merchant");
        return Row.of(
                Json.text(payment, "payment_id"),
                Json.text(payment, "account_id"),
                Json.number(payment, "amount"),
                Json.text(payment, "currency"),
                Json.text(merchant, "id"),
                Json.text(merchant, "name"),
                Json.optionalText(merchant, "category"),
                Json.text(payment, "channel"),
                Json.integer(payment, "created_at"));
    }

    static final class Enrich implements MapFunction<Row, Row> {

        private static final Map<String, Double> USD_RATES =
                Map.of("USD", 1.0, "EUR", 1.08, "GBP", 1.27, "JPY", 0.0067, "CAD", 0.73, "CHF", 1.12);

        private final boolean npeOnNullField;

        Enrich(boolean npeOnNullField) {
            this.npeOnNullField = npeOnNullField;
        }

        @Override
        public Row map(Row payment) {
            double amount = (Double) payment.getField(2);
            String currency = (String) payment.getField(3);
            String category = (String) payment.getField(6);

            double amountUsd = amount * USD_RATES.getOrDefault(currency, 1.0);
            String normalizedCategory = npeOnNullField
                    // The "v2" change: categories are always present... aren't they?
                    ? category.trim().toUpperCase()
                    : category == null ? "UNCATEGORIZED" : category.trim().toUpperCase();
            String riskBand = amountUsd >= 10_000 ? "HIGH" : amountUsd >= 1_000 ? "MEDIUM" : "LOW";

            return Row.of(
                    payment.getField(0), payment.getField(1), amount, currency, amountUsd,
                    payment.getField(4), payment.getField(5), normalizedCategory,
                    payment.getField(7), riskBand, payment.getField(8));
        }
    }

    private PaymentsEnrichment() {}
}
