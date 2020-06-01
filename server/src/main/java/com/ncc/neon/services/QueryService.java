package com.ncc.neon.services;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.ActionResult;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.models.results.TableWithFields;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class QueryService {

    private QueryAdapterLocator queryAdapterLocator;

    private ClusterService clusterService;

    @Autowired
    QueryService(QueryAdapterLocator queryExecutorLocator, ClusterService clusterService) {
        this.queryAdapterLocator = queryExecutorLocator;
        this.clusterService = clusterService;
    }

    public Mono<TabularQueryResult> executeQuery(ConnectionInfo ci, Query query) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);

        if (query.getClusterClause() != null) {
            this.clusterService.setClusterClause(query.getClusterClause());
            return adapter.execute(query).flatMap(this.clusterService::clusterIntoMono);
        } else {
            return adapter.execute(query);
        }
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

    public Mono<ActionResult> importData(ConnectionInfo ci, ImportQuery importQuery){
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.importData(importQuery);
    }

    public Mono<ActionResult> mutateData(ConnectionInfo ci, MutateQuery mutateQuery){
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.mutateData(mutateQuery);
    }

    public Mono<ActionResult> deleteData(ConnectionInfo ci, MutateQuery mutateQuery) {
        QueryAdapter adapter = this.queryAdapterLocator.getAdapter(ci);
        return adapter.deleteData(mutateQuery);
    }
}
