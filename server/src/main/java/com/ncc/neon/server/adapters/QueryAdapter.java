package com.ncc.neon.server.adapters;

import com.ncc.neon.server.models.queries.Query;
import com.ncc.neon.server.models.results.FieldTypePair;
import com.ncc.neon.server.models.results.TabularQueryResult;
import com.ncc.neon.server.models.results.TableWithFields;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface QueryAdapter {

    /**
     * Executes a query against a generic data source given the current filter and
     * selection state.
     * 
     * @param query   An object that represents the query we wish to execute
     * @param options Determines if we should include filters or selection in the
     *                query execution
     * @return An object containing the results of the query
     */
    Mono<TabularQueryResult> execute(Query query);

    /**
     * @return Returns all the databases
     */
    Flux<String> showDatabases();

    /**
     * @param dbName The current database
     * @return Returns all the table names within the current database
     */
    Flux<String> showTables(String dbName);

    /**
     * Gets the names of the fields in the specified dataset
     * 
     * @param databaseName
     * @param tableName
     * @return
     */
    Flux<String> getFieldNames(String databaseName, String tableName);

    /**
     * Gets the types of the fields in the specified dataset
     * 
     * @param databaseName
     * @param tableName
     * @return Mapping of field name to field type
     */
    Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName);

    /**
     * Gets the tables and fields for a specified dataset
     * 
     * @param databaseName
     * @return Mapping of table names to field names
     */
    Flux<TableWithFields> getTableAndFieldNames(String databaseName);
}
