package com.ncc.neon.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

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
    private final ReentrantLock lock = new ReentrantLock();

    QueryAdapterLocator(List<QueryAdapterFactory> queryAdapterFactories) throws Exception {
        if (queryAdapterFactories.size() == 0) {
            log.error("Must have at least one factory");
            throw new Exception("Must have at least one factory");
        }

        for (QueryAdapterFactory queryAdapterFactory : queryAdapterFactories) {
            for (String name : queryAdapterFactory.getNames()) {
                log.debug("Found Query Adapter Factory:  " + name);
                initialContext.put(name, queryAdapterFactory);
            }
        }
    }

    QueryAdapter getAdapter(ConnectionInfo ci) {
        lock.lock();
        try {
            QueryAdapter adapter = cache.get(ci);

            if (adapter != null) {
                return adapter;
            }

            QueryAdapterFactory adapterFactory = initialContext.get(ci.getDatabaseType());
            adapter = adapterFactory.initialize(ci);
            cache.put(ci, adapter);
            return adapter;
        }
        finally {
            lock.unlock();
        }
    }
}
