package com.ncc.neon.services;

import java.util.List;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.ImportResult;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.models.results.TableWithFields;

import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class QueryService {

    private QueryAdapterLocator queryAdapterLocator;

    QueryService(QueryAdapterLocator queryExecutorLocator) {
        this.queryAdapterLocator = queryExecutorLocator;
    }

    public Mono<TabularQueryResult> executeQuery(ConnectionInfo ci, Query query) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.execute(query);
    }

    public Flux<String> getDatabaseNames(ConnectionInfo ci) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.showDatabases();
    }

    public Flux<String> getTableNames(ConnectionInfo ci, String databaseName) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.showTables(databaseName);
    }

    public Flux<FieldTypePair> getFieldTypes(ConnectionInfo ci, String databaseName, String tableName) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.getFieldTypes(databaseName, tableName);
    }

    public Flux<String> getFields(ConnectionInfo ci, String databaseName, String tableName) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.getFieldNames(databaseName, tableName);
    }

    public Flux<TableWithFields> getTablesAndFields(ConnectionInfo ci, String databaseName) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.getTableAndFieldNames(databaseName);
    }

    public Mono<ImportResult> addData(ConnectionInfo ci, String databaseName, String table, List<String> sourceData){
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.addData(databaseName, table, sourceData);
    }
}
