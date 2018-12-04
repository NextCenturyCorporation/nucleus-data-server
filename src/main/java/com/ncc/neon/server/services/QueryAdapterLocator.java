package com.ncc.neon.server.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ncc.neon.server.models.connection.ConnectionInfo;
import com.ncc.neon.server.services.adapters.QueryAdapter;
import com.ncc.neon.server.services.adapters.QueryAdapterFactory;

/**
 * QueryExecutorLocator
 */
public class QueryAdapterLocator {


    private final Map<String, QueryAdapterFactory> initialContext = new HashMap<>();
    private final Map<ConnectionInfo, QueryAdapter> cache = new HashMap<>();

    QueryAdapterLocator(List<QueryAdapterFactory> queryAdapterFactories) {
        for (QueryAdapterFactory queryAdapterFactory : queryAdapterFactories) {
            initialContext.put(queryAdapterFactory.getName(), queryAdapterFactory);
        }
    }

    QueryAdapter getAdapter(ConnectionInfo ci) {

        QueryAdapter adapter = cache.get(ci);

        if (adapter != null) {
            return adapter;
        }

        QueryAdapterFactory adapterFactory = initialContext.get(ci.getDatabaseType());
        adapter = adapterFactory.initialize(ci);
        cache.put(ci, adapter);
        return adapter;
    }
}