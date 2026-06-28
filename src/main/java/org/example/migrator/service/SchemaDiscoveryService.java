package org.example.migrator.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;

@Service
public class SchemaDiscoveryService {

    private final JdbcTemplate jdbcTemplate;

    public SchemaDiscoveryService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String, String> getTableSchema(String tableName) {
        String sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE LOWER(table_name) = LOWER(?) AND table_schema = 'public'
    """;
        Map<String, String> schema = new HashMap<>();
        jdbcTemplate.query(sql, rs -> {
            schema.put(
                    rs.getString("column_name").toLowerCase(),
                    rs.getString("data_type").toLowerCase()
            );
        }, tableName);
        return schema;
    }
}