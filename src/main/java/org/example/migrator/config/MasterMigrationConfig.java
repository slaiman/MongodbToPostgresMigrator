package org.example.migrator.config;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.example.migrator.service.SchemaDiscoveryService;
import org.example.migrator.util.Utils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import javax.sql.DataSource;
import java.util.*;

@Configuration
public class MasterMigrationConfig {

    @Value("#{'${migration.tables}'.split(',')}")
    private List<String> tablesToMigrate;

    private final DataSource dataSource;
    private final MongoTemplate mongoTemplate;
    private final SchemaDiscoveryService schemaDiscoveryService;
    private final JdbcTemplate jdbcTemplate;

    public MasterMigrationConfig(DataSource dataSource, MongoTemplate mongoTemplate, SchemaDiscoveryService schemaDiscoveryService, JdbcTemplate jdbcTemplate) {
        this.dataSource = dataSource;
        this.mongoTemplate = mongoTemplate;
        this.schemaDiscoveryService = schemaDiscoveryService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Bean
    public Job masterMigrationJob(JobRepository jr, PlatformTransactionManager tm) {
        if (tablesToMigrate == null || tablesToMigrate.isEmpty()) {
            throw new IllegalStateException("The 'migration.tables' property is empty or missing in application.properties!");
        }
        String firstTable = tablesToMigrate.get(0).trim();
        SimpleJobBuilder jobBuilder = new JobBuilder("masterMigrationJob", jr).start(buildPurelyDynamicStep(firstTable, jr, tm));
        for (int i = 1; i < tablesToMigrate.size(); i++) {
            String nextTable = tablesToMigrate.get(i).trim();
            jobBuilder.next(buildPurelyDynamicStep(nextTable, jr, tm));
        }
        return jobBuilder.build();
    }

    public Step buildPurelyDynamicStep(String collectionName, JobRepository jr, PlatformTransactionManager tm) {
        ensurePostgresTableExists(collectionName);
        Map<String, String> columnTypes = schemaDiscoveryService.getTableSchema(collectionName);
        if (columnTypes.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "Could not discover schema for table '%s'. Please check if the table exists in PostgresSQL and is in the 'public' schema.",
                    collectionName
            ));
        }
        String trueInsertSql = Utils.generateInsertSqlFromSchema(collectionName, columnTypes.keySet());

        JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriterBuilder<Map<String, Object>>()
                .dataSource(dataSource)
                .sql(trueInsertSql)
                .itemSqlParameterSourceProvider(MapSqlParameterSource::new)
                .build();

        try { writer.afterPropertiesSet(); } catch (Exception e) { throw new RuntimeException(e); }

        return new StepBuilder(collectionName + "MigrationStep", jr).<Document, Map<String, Object>>chunk(1000, tm)
                .reader(createMongoStreamingReader(collectionName))
                .processor(bsonDoc -> {
                    Map<String, Object> sqlData = new HashMap<>();
                    for (String column : columnTypes.keySet()) {
                        sqlData.put(column, null);
                    }
                    for (String key : bsonDoc.keySet()) {
                        String cleanKey = Utils.sanitizeKey(key);
                        Object value = bsonDoc.get(key);
                        if (columnTypes.containsKey(cleanKey)) {
                            sqlData.put(cleanKey, Utils.castToPostgresType(value, columnTypes.get(cleanKey)));
                        }
                    }
                    return sqlData;
                })
                .writer(writer)
                .build();
    }

    private void ensurePostgresTableExists(String collectionName) {
        MongoCollection<Document> collection = mongoTemplate.getDb().getCollection(collectionName);
        Document sampleDoc = collection.find().first();

        if (sampleDoc == null) {
            String fallbackSql = String.format("CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY)", collectionName);
            jdbcTemplate.execute(fallbackSql);
            return;
        }
        List<String> columnDefinitions = new ArrayList<>();
        columnDefinitions.add("id TEXT PRIMARY KEY");
        for (String key : sampleDoc.keySet()) {
            String cleanKey = Utils.sanitizeKey(key);
            if ("id".equals(cleanKey)) {
                continue;
            }
            Object value = sampleDoc.get(key);
            String pgType = Utils.determinePostgresType(value);
            columnDefinitions.add(String.format("%s %s", cleanKey, pgType));
        }
        String createTableSql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", collectionName, String.join(", ", columnDefinitions)
        );
        jdbcTemplate.execute(createTableSql);
    }

    private ItemReader<Document> createMongoStreamingReader(String collectionName) {
        return new ItemReader<>() {
            private MongoCursor<Document> cursor = null;

            @Override
            public org.bson.Document read() {
                if (cursor == null) {
                    MongoCollection<Document> collection = mongoTemplate.getDb().getCollection(collectionName);
                    cursor = collection.find().batchSize(1000).iterator();
                }
                return cursor.hasNext() ? cursor.next() : null;
            }
        };
    }
}