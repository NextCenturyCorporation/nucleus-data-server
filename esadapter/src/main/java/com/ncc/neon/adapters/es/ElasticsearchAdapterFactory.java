package com.ncc.neon.adapters.es;

import java.util.Map;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;
import com.ncc.neon.models.ConnectionInfo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource(value="classpath:server.properties",ignoreResourceNotFound=true)
public class ElasticsearchAdapterFactory extends QueryAdapterFactory {

    public ElasticsearchAdapterFactory(final @Value("#{${elasticsearch.auth:{}}}") Map<String, String> authCollection) {
        super("Elasticsearch", authCollection);
    }

    @Override
    public QueryAdapter buildAdapter(String host, String username, String password) {
        return new ElasticsearchAdapter(host, username, password);
    }

    @Override
    public String[] getNames() {
        // Add "elasticsearchrest" for backwards compatibility
        return new String[] { "elasticsearch", "elasticsearchrest" };
    }
}
