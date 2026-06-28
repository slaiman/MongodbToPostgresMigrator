package org.example.migrator.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    public static String generateInsertSqlFromSchema(String tableName, Collection<String> columns) {
        List<String> colList = new ArrayList<>(columns);
        List<String> placeholders = colList.stream()
                .map(col -> ":" + col)
                .toList();
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO NOTHING",
                tableName,
                String.join(", ", colList),
                String.join(", ", placeholders)
        );
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
