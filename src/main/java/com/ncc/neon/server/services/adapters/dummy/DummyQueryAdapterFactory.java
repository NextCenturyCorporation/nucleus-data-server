package com.ncc.neon.server.services.adapters.dummy;

import com.ncc.neon.server.models.connection.ConnectionInfo;
import com.ncc.neon.server.services.adapters.QueryAdapter;
import com.ncc.neon.server.services.adapters.QueryAdapterFactory;

import org.springframework.stereotype.Component;

/**
 * DummyQueryAdapterFactory
 */
@Component
public class DummyQueryAdapterFactory implements QueryAdapterFactory {

    @Override
    public String getName() {
        return "elasticsearchrest";
    }

    @Override
    public QueryAdapter initialize(ConnectionInfo cInfo) {
        return new DummyQueryAdapter();
    }

}