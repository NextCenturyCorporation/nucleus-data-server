package com.ncc.neon.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;
import com.ncc.neon.models.ConnectionInfo;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class QueryAdapterLocator {

    private final Map<String, QueryAdapterFactory> initialContext = new HashMap<>();
    private final Map<ConnectionInfo, QueryAdapter> cache = new HashMap<>();

    QueryAdapterLocator(List<QueryAdapterFactory> queryAdapterFactories) throws Exception {
        if (queryAdapterFactories.size() == 0) {
            log.error("Must have at least one factory");
            throw new Exception("Must have at least one factory");
        }

        for (QueryAdapterFactory queryAdapterFactory : queryAdapterFactories) {
            log.debug("Loaded Query Adapter:  " + queryAdapterFactory.getName());
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
