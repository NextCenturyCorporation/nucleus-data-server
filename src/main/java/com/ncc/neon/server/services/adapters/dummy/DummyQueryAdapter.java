package com.ncc.neon.server.services.adapters.dummy;

import java.util.Map;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.result.TabularQueryResult;
import com.ncc.neon.server.services.adapters.QueryAdapter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * DummyQueryAdapter
 */
public class DummyQueryAdapter implements QueryAdapter {

    @Override
    public Mono<TabularQueryResult> execute(Query query, QueryOptions options) {
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
	public Mono<Map<String, String>> getFieldTypes(String databaseName, String tableName) {
		return null;
	}

    
}