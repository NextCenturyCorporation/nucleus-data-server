package com.ncc.neon.server.services;

import java.util.List;
import java.util.Map;

import com.ncc.neon.server.models.connection.ConnectionInfo;
import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.result.TabularQueryResult;
import com.ncc.neon.server.services.adapters.QueryAdapter;

import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

/**
 * QueryService
 */
@Component
public class QueryService {

    private QueryAdapterLocator queryExecutorLocator;

    QueryService(QueryAdapterLocator queryExecutorLocator) {
        this.queryExecutorLocator = queryExecutorLocator;
    }

    public Mono<TabularQueryResult> executeQuery(ConnectionInfo ci, Query query, QueryOptions options) {
        QueryAdapter adapter = this.queryExecutorLocator.getAdapter(ci);
        return adapter.execute(query, options);
    }
   
    public Flux<String> getDatabaseNames(ConnectionInfo ci) {
        QueryAdapter adapter = this.queryExecutorLocator.getAdapter(ci);
        return adapter.showDatabases();
    }

    public Flux<String> getTableNames(ConnectionInfo ci, String databaseName) {
        QueryAdapter adapter = this.queryExecutorLocator.getAdapter(ci);
        return adapter.showTables(databaseName);
    }

    public Mono<Map<String, String>> getFieldTypes(ConnectionInfo ci, String databaseName, String tableName) {
        QueryAdapter adapter = this.queryExecutorLocator.getAdapter(ci);
        return adapter.getFieldTypes(databaseName, tableName);
    }

    // TODO: correct signature?
    public Flux<String> getFields(ConnectionInfo ci, String databaseName, String tableName) {
        QueryAdapter adapter = this.queryExecutorLocator.getAdapter(ci);
        return adapter.getFieldNames(databaseName, tableName);
    }

    // TODO: examine this?
    public Flux<Tuple2<String, List<String>>> getTablesAndFields() {
        return null;
    }
}