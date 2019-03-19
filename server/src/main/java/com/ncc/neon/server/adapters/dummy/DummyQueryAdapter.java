package com.ncc.neon.server.adapters.dummy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ncc.neon.server.adapters.QueryAdapter;
import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.result.FieldTypePair;
import com.ncc.neon.server.models.query.result.TableWithFields;
import com.ncc.neon.server.models.query.result.TabularQueryResult;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * DummyQueryAdapter
 */
public class DummyQueryAdapter implements QueryAdapter {

    @Override
    public Mono<TabularQueryResult> execute(Query query, QueryOptions options) {
        List<Map<String, Object>> table = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put("column1", "value1");
        row.put("column2", 2);
        table.add(row);
        table.add(row);
        return Mono.just(new TabularQueryResult(table));
    }

    @Override
    public Flux<String> showDatabases() {
        return Flux.just("A", "B", "C", "X", "Y", "Z");
    }

    @Override
    public Flux<String> showTables(String dbName) {
        return Flux.just("1", "2");
    }

    @Override
    public Flux<String> getFieldNames(String databaseName, String tableName) {
        return Flux.just("id", "date", "msg");
    }

    @Override
    public Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName) {
        Flux<FieldTypePair> fieldTypes = getFieldNames("", "").map(name -> new FieldTypePair(name, name + "-type"));
        return fieldTypes;
    }

    @Override
    public Flux<TableWithFields> getTableAndFieldNames(String dbName) {
        List<String> fields = new ArrayList<String>();
        fields.add("field1");
        fields.add("field2");
        Flux<TableWithFields> tablesAndFields = showTables("").map(name -> new TableWithFields(name, fields));
        return tablesAndFields;
    }

}
