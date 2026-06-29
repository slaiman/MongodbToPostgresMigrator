package org.example.migrator.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.sql.DataSource;

@Configuration
public class DynamicPostgresConfig {

    @Value("${migration.target.postgres-db-name}")
    private String postgresDatabaseName;

    @Value("${spring.datasource.url}")
    private String postgresUrl;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Bean
    public DataSource dataSource() {
        String dynamicUrl = postgresUrl.endsWith("/") ? postgresUrl + postgresDatabaseName : postgresUrl + "/" + postgresDatabaseName;

        return DataSourceBuilder.create()
                .url(dynamicUrl)
                .username(username)
                .password(password)
                .build();
    }

}
