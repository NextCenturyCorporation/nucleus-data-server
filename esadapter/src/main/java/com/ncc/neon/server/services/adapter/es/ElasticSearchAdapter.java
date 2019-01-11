package com.ncc.neon.server.services.adapter.es;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.result.FieldTypePair;
import com.ncc.neon.server.models.query.result.TabularQueryResult;
import com.ncc.neon.server.services.adapters.QueryAdapter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * ElasticSearchAdapter
 */
public class ElasticSearchAdapter implements QueryAdapter {

    public ElasticSearchAdapter() {
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public Flux<String> showDatabases() {
        return Flux.just("data");
    }

    @Override
    public Flux<String> showTables(String dbName) {
        return null;
    }

    @Override
    public Flux<String> getFieldNames(String databaseName, String tableName) {
        return null;
    }

    @Override
    public Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName) {
        return null;
    }

}