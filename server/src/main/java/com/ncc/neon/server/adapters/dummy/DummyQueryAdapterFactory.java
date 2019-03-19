package com.ncc.neon.server.adapters.dummy;

import com.ncc.neon.server.adapters.QueryAdapter;
import com.ncc.neon.server.adapters.QueryAdapterFactory;
import com.ncc.neon.server.models.ConnectionInfo;

import org.springframework.stereotype.Component;

/**
 * DummyQueryAdapterFactory
 */
@Component
public class DummyQueryAdapterFactory implements QueryAdapterFactory {

    @Override
    public String getName() {
        return "dummy";
    }

    @Override
    public QueryAdapter initialize(ConnectionInfo cInfo) {
        return new DummyQueryAdapter();
    }

}
