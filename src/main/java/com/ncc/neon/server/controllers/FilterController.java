package com.ncc.neon.server.controllers;

import java.util.List;

import com.ncc.neon.server.models.query.filter.Filter;
import com.ncc.neon.server.models.query.filter.FilterEvent;
import com.ncc.neon.server.models.query.filter.FilterKey;
import com.ncc.neon.server.stores.FilterState;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

/**
 * Service for executing queries against an arbitrary data store.
 */
@RestController
@RequestMapping("filterservice")
public class FilterController {

    private FilterState filterState;

    @Autowired
    public FilterController(FilterState filterState) {
        this.filterState = filterState;
    }

    /**
     * Creates and returns an empty filter containing only the given database and
     * table names
     * 
     * @param databaseName
     * @param tableName
     * @return
     */
    private Filter createEmptyFilter(String databaseName, String tableName) {
        return new Filter(databaseName, tableName);
    }

    /**
     * Add a filter
     * 
     * @param filterKey The filter to add
     * @return an ADD filter event
     */
    @PostMapping(path = "addfilter", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<FilterEvent> addFilter(@RequestBody FilterKey filterKey) {
        /*
         * filterState.addFilter(filterKey) Filter added = filterKey.filter Filter
         * removed = createEmptyFilter(added.databaseName, added.tableName) return new
         * FilterEvent(type: "ADD", addedFilter: added, removedFilter: removed)
         */
        return null;
    }

    /**
     * Removes the filters associated with the specified id
     * 
     * @param filterId The id of the filter to remove
     * @return a REMOVE filter event
     */
    @PostMapping(path = "removefilter", consumes = MediaType.TEXT_PLAIN_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    FilterEvent removeFilter(String id) {
        /*
         * FilterKey filterKey = filterState.removeFilter(id) Filter removed =
         * filterKey?.filter ?: createEmptyFilter("", "") Filter added =
         * createEmptyFilter(removed.databaseName, removed.tableName) return new
         * FilterEvent(type: "REMOVE", addedFilter: added, removedFilter: removed)
         */

        return null;
    }

    /**
     * Replace the filters for the given filter key. If none exists, this works the
     * same as addFilter(filterKey)
     * 
     * @param filterKey The filter to replace
     * @return a REPLACE filter event
     */
    @PostMapping(path = "replacefilter", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    FilterEvent replaceFilter(FilterKey filterKey) {
        /*
         * Filter removed = removeFilter(filterKey.id).removedFilter Filter added =
         * addFilter(filterKey).addedFilter return new FilterEvent(type: "REPLACE",
         * addedFilter: added, removedFilter: removed)
         */
        return null;
    }

    /**
     * Clears all filters.
     * 
     * @return a CLEAR filter event
     */
    @PostMapping(path = "clearfilters", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    FilterEvent clearFilters() {
        /*
         * filterState.clearAllFilters() // use an empty dataset since the clear can
         * span multiple datasets Filter empty = createEmptyFilter("", "") return new
         * FilterEvent(type: "CLEAR", addedFilter: empty, removedFilter: empty)
         */
        return null;
    }

    /**
     * Get all filters for a given table.
     * 
     * @param tableName
     * @return
     */
    @GetMapping(path = "filters/{databaseName}/{tableName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    List getFilters(@PathVariable String databaseName, @PathVariable String tableName) {
        /*
         * if(databaseName == "*" && tableName == "*") { return
         * filterState.getAllFilterKeys() } else if(databaseName == "*") { return
         * filterState.getFilterKeysForTables(tableName) } else if(tableName == "*") {
         * return filterState.getFilterKeysForDatabase(databaseName) }
         * 
         * DataSet dataset = new DataSet(databaseName: databaseName, tableName:
         * tableName) return filterState.getFilterKeysForDataset(dataset)
         */
        return null;
    }
}