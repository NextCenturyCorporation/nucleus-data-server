package com.ncc.neon.server.controllers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryResult;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

/**
 * Service for executing queries against an arbitrary data store.
 */
@RestController
@RequestMapping("queryservice")
public class QueryController {
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
    Mono<QueryResult> executeQuery(@PathVariable String host, @PathVariable String databaseType,
            @RequestParam(value = "ignoreFilters", defaultValue = "false") boolean ignoreFilters,
            @RequestParam(value = "selectionOnly", defaultValue = "false") boolean selectionOnly,
            @RequestParam(value = "ignoreFilterIds", defaultValue = "false") Set<String> ignoreFilterIds,
            @RequestBody Query query) {
        System.out.print(query.getQuery());
        return Mono.just(new QueryResult(host + databaseType + ignoreFilters + query.getQuery()));
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
        return Mono.just(Arrays.asList("Earth", "Mars", "Venus"));
    }

    /**
     * Get all the columns for all the tables from the supplied connection.
     * 
     * @param host         The host the database is running on
     * @param databaseType the type of database
     * @param databaseName The database containing the data
     * @return The result of the query
     */
    @GetMapping(path = "tableandfields/{host}/{databaseType}/{databaseName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Map<String, List<String>>> getTablesAndFields(@PathVariable String host, @PathVariable String databaseType,
            @PathVariable String databaseName) {
        List<String> list = Arrays.asList("Earth", "Mars", "Venus");
        Map<String, List<String>> map = new HashMap<>();
        map.put("x", list);
        map.put("y", list);
        map.put("z", list);
        return Mono.just(map);

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
        return Mono.just(Arrays.asList("Earth", "Mars", "Venus"));
    }

    /**
     * Gets a list of all the tables for the supplied connection
     * 
     * @param host         The host the database is running on
     * @param databaseType the type of database
     * @param database     The database that contains the tables
     * @return The list of table names
     */
    @GetMapping(path = "tablenames/{host}/{databaseType}/{database}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<List<String>> getFieldTypes(@PathVariable String host, @PathVariable String databaseType,
            @PathVariable String database) {
        return Mono.just(Arrays.asList("Earth", "Mars", "Venus"));
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

    // TODO: Type this
    @GetMapping(path = "fields/types/{host}/{databaseType}/{databaseName}/{tableName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Map> getTableNames(@PathVariable String host, @PathVariable String databaseType,
            @PathVariable String databaseName, @PathVariable String tableName) {
        List<String> list = Arrays.asList("Earth", "Mars", "Venus");
        Map<String, List<String>> map = new HashMap<>();
        map.put("x", list);
        map.put("y", list);
        map.put("z", list);
        return Mono.just(map);
    }

}