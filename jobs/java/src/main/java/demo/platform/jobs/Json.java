package demo.platform.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Strict JSON field access: a missing or mistyped required field throws. */
final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static JsonNode read(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("expected a JSON object");
        }
        return node;
    }

    static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) {
            throw new IllegalArgumentException("field '%s' must be a string, got %s".formatted(field, describe(value)));
        }
        return value.asText();
    }

    /** A string field that may be absent or null. */
    static String optionalText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new IllegalArgumentException("field '%s' must be a string or null, got %s".formatted(field, describe(value)));
        }
        return value.asText();
    }

    static long integer(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber()) {
            throw new IllegalArgumentException("field '%s' must be an integer, got %s".formatted(field, describe(value)));
        }
        return value.asLong();
    }

    static double number(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isNumber()) {
            throw new IllegalArgumentException("field '%s' must be a number, got %s".formatted(field, describe(value)));
        }
        return value.asDouble();
    }

    static JsonNode object(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isObject()) {
            throw new IllegalArgumentException("field '%s' must be an object, got %s".formatted(field, describe(value)));
        }
        return value;
    }

    private static String describe(JsonNode value) {
        return value == null ? "nothing" : value.getNodeType().name().toLowerCase() + " " + value;
    }

    private Json() {}
}
