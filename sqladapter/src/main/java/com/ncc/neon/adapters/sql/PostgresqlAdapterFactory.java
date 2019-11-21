package com.ncc.neon.adapters.sql;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value="classpath:server.properties",ignoreResourceNotFound=true)
public class PostgresqlAdapterFactory extends SqlAdapterFactory {
    public PostgresqlAdapterFactory(final @Value("#{${postgresql.auth:{}}}") Map<String, String> authCollection) {
        super(SqlType.POSTGRESQL, authCollection);
    }
}
