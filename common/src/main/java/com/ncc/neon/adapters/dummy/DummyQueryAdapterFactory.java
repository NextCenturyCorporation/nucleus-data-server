package com.ncc.neon.adapters.dummy;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;
import com.ncc.neon.models.ConnectionInfo;

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
