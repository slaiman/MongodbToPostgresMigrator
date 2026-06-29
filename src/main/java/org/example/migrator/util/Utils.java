package org.example.migrator.util;

import java.util.Map;

public final class Utils {

    private Utils() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    public static Object castToPostgresType(Object value, String pgDataType) {
        if (value == null) return null;

        switch (pgDataType) {
            case "boolean":
                if (value instanceof String) return Boolean.parseBoolean((String) value);
                return value;

            case "integer":
            case "bigint":
                if (value instanceof Number) return ((Number) value).longValue();
                if (value instanceof String) return Long.parseLong((String) value);
                return value;

            case "jsonb":
            case "text":
                if (value instanceof Map || value instanceof java.util.List) {
                    return value.toString();
                }
                return value.toString();

            default:
                return value;
        }
    }

    public static String determinePostgresType(Object value) {
        if (value instanceof Boolean) return "BOOLEAN";
        return "TEXT";
    }

    public static String sanitizeKey(String key) {
        if ("_id".equalsIgnoreCase(key)) {
            return "id";
        }
        String clean = key.toLowerCase();
        clean = clean.replaceAll("[^a-z0-9_]+", "_");
        clean = clean.replaceAll("^_+|_+$", "");
        return clean;
    }
}
