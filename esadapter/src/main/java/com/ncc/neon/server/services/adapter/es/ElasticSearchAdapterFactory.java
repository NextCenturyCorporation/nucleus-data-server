package com.ncc.neon.server.services.adapter.es;

import com.ncc.neon.server.models.connection.ConnectionInfo;
import com.ncc.neon.server.services.adapters.QueryAdapter;
import com.ncc.neon.server.services.adapters.QueryAdapterFactory;

import org.springframework.stereotype.Component;

//import org.springframework.stereotype.Component;

/**
 * ElasticSearchAdapterFactory
 */
@Component
public class ElasticSearchAdapterFactory implements QueryAdapterFactory {

    @Override
    public String getName() {
        return "elasticsearchrest1";
    }

    @Override
    public QueryAdapter initialize(ConnectionInfo cInfo) {
        return new ElasticSearchAdapter();
    }

}