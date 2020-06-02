package com.ncc.neon.adapters.dummy;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Map;

public class DummyQueryAdapter extends QueryAdapter {

    DummyQueryAdapter() {
        super("dummy", null, null, null);
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query) {
        return Mono.just(new TabularQueryResult(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "value1"),
                Map.entry("testAggregateLabel", 1)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "value2"),
                Map.entry("testAggregateLabel", 2)
            )
        )));
    }

    @Override
    public Flux<String> showDatabases() {
        return Flux.just("A", "B", "C", "D");
    }

    @Override
    public Flux<String> showTables(String dbName) {
        return Flux.just("X", "Y");
    }

    @Override
    public Flux<String> getFieldNames(String databaseName, String tableName) {
        return Flux.just("id", "blob", "date", "flag", "name", "size", "text", "type");
    }

    @Override
    public Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName) {
        return Flux.just(
            new FieldTypePair("id", FieldType.ID),
            new FieldTypePair("blob", FieldType.OBJECT),
            new FieldTypePair("date", FieldType.DATETIME),
            new FieldTypePair("flag", FieldType.BOOLEAN),
            new FieldTypePair("name", FieldType.KEYWORD),
            new FieldTypePair("size", FieldType.DECIMAL),
            new FieldTypePair("text", FieldType.TEXT),
            new FieldTypePair("type", FieldType.KEYWORD)
        );
    }

    @Override
    public Flux<TableWithFields> getTableAndFieldNames(String dbName) {
        // TODO Deprecated
        return Flux.just();
    }

    @Override
    public Mono<ActionResult> importData(ImportQuery importQuery) {
        return Mono.just(new ActionResult());
    }

    @Override
    public Mono<ActionResult> mutateData(MutateQuery mutate) {
        return Mono.just(new ActionResult());
    }

    @Override
    public Mono<ActionResult> insertData(MutateQuery mutate) {
        return Mono.just(new ActionResult());
    }

    @Override
    public Mono<ActionResult> deleteData(MutateQuery mutate) {
        return Mono.just(new ActionResult());
    }
}
