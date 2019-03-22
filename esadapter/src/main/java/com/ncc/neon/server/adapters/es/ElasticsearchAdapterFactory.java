package com.ncc.neon.server.adapters.es;

import com.ncc.neon.server.adapters.QueryAdapter;
import com.ncc.neon.server.adapters.QueryAdapterFactory;
import com.ncc.neon.server.models.ConnectionInfo;

import org.springframework.stereotype.Component;

@Component
public class ElasticsearchAdapterFactory implements QueryAdapterFactory {

    @Override
    public String getName() {
        return "elasticsearchrest";
    }

    @Override
    public QueryAdapter initialize(ConnectionInfo cInfo) {
        return new ElasticsearchAdapter(cInfo.getHost());
    }

}
