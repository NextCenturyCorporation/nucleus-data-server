package com.ncc.neon.adapters.sparql;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.ActionResult;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.TableWithFields;
import com.ncc.neon.models.results.TabularQueryResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SparqlAdapter extends QueryAdapter {

    public SparqlAdapter(String host, String usernameFromConfig, String passwordFromConfig) {
        super("Sparql", host, usernameFromConfig, passwordFromConfig);
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query) {
        return null;
    }

    @Override
    public Flux<String> showDatabases() {
        return null;
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

    @Override
    public Flux<TableWithFields> getTableAndFieldNames(String databaseName) {
        return null;
    }

    @Override
    public Mono<ActionResult> importData(ImportQuery importQuery) {
        return null;
    }

    @Override
    public Mono<ActionResult> mutateData(MutateQuery mutate) {
        return null;
    }

    @Override
    public Mono<ActionResult> insertData(MutateQuery mutate) {
        return null;
    }

    @Override
    public Mono<ActionResult> deleteData(MutateQuery mutate) {
        return null;
    }
}
