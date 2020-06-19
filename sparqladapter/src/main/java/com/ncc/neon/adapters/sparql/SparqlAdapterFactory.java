package com.ncc.neon.adapters.sparql;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@PropertySource(value="classpath:server.properties",ignoreResourceNotFound=true)
public class SparqlAdapterFactory extends QueryAdapterFactory {

    public SparqlAdapterFactory(final @Value("#{${sparql.auth:{}}}") Map<String, String> authCollection) {
        super("Sparql", authCollection);
    }

    @Override
    public QueryAdapter buildAdapter(String host, String username, String password) {
        return new SparqlAdapter(host, username, password);
    }

    @Override
    public String[] getNames() {
        return new String[] { "sparql" };
    }
}
