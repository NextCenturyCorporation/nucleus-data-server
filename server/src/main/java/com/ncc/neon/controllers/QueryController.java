package com.ncc.neon.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.TabularQueryResult;
import com.ncc.neon.services.QueryService;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;
import lombok.extern.slf4j.Slf4j;

@CrossOrigin(origins="*")
@RestController
@RequestMapping("queryservice")
@Slf4j
public class QueryController {

    private QueryService queryService;

    QueryController(QueryService queryService) {
        this.queryService = queryService;
    }

    private static void logObject(String name, Object object) {
        log.debug(name + ":  " + object.toString());
    }

    /**
     * Executes a query against the supplied connection. This takes into account the
     * user's current filters so the results will be limited by these if they exist.
     * 
     * @param host             The host the database is running on
     * @param databaseType     the type of database
     * @param includeFilters   If filters should be ignored and all data should be
     *                         returned. Defaults to false.
     * @param selectionOnly    If only data that is currently selected should be
     *                         returned. Defaults to false.
     * @param ignoredFilterIds If any specific filters should be ignored (only used
     *                         if includeFilters is false)
     * @param query            The query being executed
     * @return The result of the query
     */
    @PostMapping(path = "query/{host}/{databaseType}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<TabularQueryResult> executeQuery(@PathVariable String host, @PathVariable String databaseType,
            @RequestParam(value = "ignoreFilters", defaultValue = "false") boolean ignoreFilters,
            @RequestParam(value = "selectionOnly", defaultValue = "false") boolean selectionOnly,
            @RequestParam(value = "ignoreFilterIds", defaultValue = "false") Set<String> ignoreFilterIds,
            @RequestBody Query query) {
        // TODO THOR-1088 Remove unused request parameters!
        ConnectionInfo ci = new ConnectionInfo(databaseType, host);
        return queryService.executeQuery(ci, query);
    }

    /**
     * Gets a list of all the databases for the database type/host pair.
     * 
     * @param host         The host the database is running on
     * @param databaseType the type of database
     * @return The list of database names
     */
    @GetMapping(path = "databasenames/{host}/{databaseType}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<List<String>> getDatabaseNames(@PathVariable String host, @PathVariable String databaseType) {
        ConnectionInfo ci = new ConnectionInfo(databaseType, host);
        return queryService.getDatabaseNames(ci).collectList();
    }

    /**
     * Get all the column's types for tabular datasets from the supplied connection.
     * 
     * @param host         The host the database is running on
     * @param databaseType the type of database
     * @param databaseName The database containing the data
     * @param tableName    The table containing the data
     * @return The field names and their types.
     */
    @GetMapping(path = "tablenames/{host}/{databaseType}/{databaseName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<List<String>> getTableNames(@PathVariable String host, @PathVariable String databaseType,
            @PathVariable String databaseName) {
        ConnectionInfo ci = new ConnectionInfo(databaseType, host);
        return queryService.getTableNames(ci, databaseName).collectList();
    }

    /**
     * Get all the columns for tabular datasets from the supplied connection.
     * 
     * @param host         The host the database is running on
     * @param databaseType the type of database
     * @param databaseName The database containing the data
     * @param tableName    The table containing the data
     * @return The result of the query
     */
    @GetMapping(path = "fields/{host}/{databaseType}/{databaseName}/{tableName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<List<String>> getFields(@PathVariable String host, @PathVariable String databaseType,
            @PathVariable String databaseName, @PathVariable String tableName) {
        ConnectionInfo ci = new ConnectionInfo(databaseType, host);
        return queryService.getFields(ci, databaseName, tableName).collectList();
    }

    /**
     * Gets a list of all the tables for the supplied connection
     * 
     * @param host         The host the database is running on
     * @param databaseType the type of database
     * @param database     The database that contains the tables
     * @return The list of table names
     */
    @GetMapping(path = "fields/types/{host}/{databaseType}/{databaseName}/{tableName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Map<String, String>> getFieldTypes(@PathVariable String host, @PathVariable String databaseType,
            @PathVariable String databaseName, @PathVariable String tableName) {
        ConnectionInfo ci = new ConnectionInfo(databaseType, host);
        return queryService.getFieldTypes(ci, databaseName, tableName).collectMap(p -> p.getField(), p -> p.getType());
    }

    /**
     * Get all the columns for all the tables from the supplied connection.
     * 
     * @param host         The host the database is running on
     * @param databaseType the type of database
     * @param databaseName The database containing the data
     * @return
     * @return The result of the query
     */
    // Spring WebFlux can only deal with one reactive type and won't deal with
    // nested reactive types (like a Mono<Flux<Integer>>
    @GetMapping(path = "tablesandfields/{host}/{databaseType}/{databaseName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Map<String, List<String>>> getTablesAndFields(@PathVariable String host, @PathVariable String databaseType,
            @PathVariable String databaseName) {
        ConnectionInfo ci = new ConnectionInfo(databaseType, host);

        return this.queryService.getTablesAndFields(ci, databaseName)
            .collect(Collectors.toMap(tableAndFields -> tableAndFields.getTableName(), tableAndFields -> {
            return tableAndFields.getFields();
        }));
    }
}
