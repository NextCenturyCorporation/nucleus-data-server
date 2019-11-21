package com.ncc.neon.adapters.sql;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value="classpath:server.properties",ignoreResourceNotFound=true)
public class MySqlAdapterFactory extends SqlAdapterFactory {
    public MySqlAdapterFactory(final @Value("#{${mysql.auth:{}}}") Map<String, String> authCollection) {
        super(SqlType.MYSQL, authCollection);
    }
}
