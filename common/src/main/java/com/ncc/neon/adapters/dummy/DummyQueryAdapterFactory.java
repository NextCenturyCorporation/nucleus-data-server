package com.ncc.neon.adapters.dummy;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;

import org.springframework.stereotype.Component;

@Component
public class DummyQueryAdapterFactory extends QueryAdapterFactory {

    DummyQueryAdapterFactory() {
        super("Dummy", null);
    }

    @Override
    public QueryAdapter buildAdapter(String host, String username, String password) {
        return new DummyQueryAdapter();
    }

    @Override
    public String[] getNames() {
        return new String[] { "dummy" };
    }
}
