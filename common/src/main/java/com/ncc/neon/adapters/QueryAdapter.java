package com.ncc.neon.adapters;

import java.util.List;
import java.util.Map;

import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.TabularQueryResult;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.ncc.neon.models.results.TableWithFields;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Data
@Slf4j
public abstract class QueryAdapter {

    protected String prettyName;
    protected String host;

    public QueryAdapter(final String prettyName, final String host, final String username, final String password) {
        this.prettyName = prettyName;
        this.host = host;
        log.debug("Initialize " + prettyName + " Adapter " + host +
            ((username != null && password != null) ? " with auth from config" : ""));
    }

    protected void logError(String origin, Exception e) {
        log.error(this.prettyName + " " + origin + " Error", e);
    }

    protected void logQuery(Query query, Object request) {
        if (query != null) {
            log.debug("Neon Query:  " + query.toString());
        }
        if (request != null) {
            log.debug(this.prettyName + " Query:  " + request.toString());
        }
    }

    protected void logResults(Object reply, List<Map<String, Object>> results) {
        if (reply != null) {
            log.debug(this.prettyName + " Results:  " + reply.toString());
        }
        if (results != null) {
            log.debug("Neon Results:  " + results.toString());
        }
    }

    /**
     * Returns whether all the tables in the given query exist.
     */
    protected void verifyQueryTablesExist(Query query) {
        if (query == null || query.getFilter() == null) {
            throw new RuntimeException("Query does not exist");
        }

        // TODO: Fix (THOR-1077) - commenting out for now
        //String tableName = query.getFilter().getTableName();
        //if (showTables(tableName).collectList().block().indexOf(tableName) >= 0) {
        //    throw new RuntimeException("Table ${tableName} does not exist");
        //}
    }

    /**
     * Executes a query against a generic data source given the current filter and
     * selection state.
     * 
     * @param query   An object that represents the query we wish to execute
     * @param options Determines if we should include filters or selection in the
     *                query execution
     * @return An object containing the results of the query
     */
    public abstract Mono<TabularQueryResult> execute(Query query);

    /**
     * @return Returns all the databases
     */
    public abstract Flux<String> showDatabases();

    /**
     * @param dbName The current database
     * @return Returns all the table names within the current database
     */
    public abstract Flux<String> showTables(String dbName);

    /**
     * Gets the names of the fields in the specified dataset
     * 
     * @param databaseName
     * @param tableName
     * @return
     */
    public abstract Flux<String> getFieldNames(String databaseName, String tableName);

    /**
     * Gets the types of the fields in the specified dataset
     * 
     * @param databaseName
     * @param tableName
     * @return Mapping of field name to field type
     */
    public abstract Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName);

    /**
     * Gets the tables and fields for a specified dataset
     * 
     * @param databaseName
     * @return Mapping of table names to field names
     */
    public abstract Flux<TableWithFields> getTableAndFieldNames(String databaseName);
}
