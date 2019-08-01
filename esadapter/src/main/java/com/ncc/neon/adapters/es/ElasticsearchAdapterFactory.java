package com.ncc.neon.adapters.es;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.adapters.QueryAdapterFactory;
import com.ncc.neon.models.ConnectionInfo;

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
