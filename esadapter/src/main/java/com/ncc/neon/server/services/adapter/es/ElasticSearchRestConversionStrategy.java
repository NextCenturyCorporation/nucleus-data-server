package com.ncc.neon.server.services.adapter.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.management.RuntimeErrorException;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.AndWhereClause;
import com.ncc.neon.server.models.query.clauses.OrWhereClause;
import com.ncc.neon.server.models.query.clauses.SingularWhereClause;
import com.ncc.neon.server.models.query.clauses.WhereClause;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ElasticSearchRestConversionStrategy
 */
public class ElasticSearchRestConversionStrategy {
    static final String TERM_PREFIX = "_term";
    static final String STATS_AGG_PREFIX = "_statsFor_";
    static final String[] DATE_OPERATIONS = { "year", "month", "dayOfMonth", "dayOfWeek", "hour", "minute", "second" };

    public static final int RESULT_LIMIT = 10000;
    private static final int TERM_AGGREGATION_SIZE = 100;

    public static final DateHistogramInterval YEAR = DateHistogramInterval.YEAR;
    public static final DateHistogramInterval MONTH = DateHistogramInterval.MONTH;
    public static final DateHistogramInterval DAY = DateHistogramInterval.DAY;
    public static final DateHistogramInterval HOUR = DateHistogramInterval.HOUR;
    public static final DateHistogramInterval MINUTE = DateHistogramInterval.MINUTE;
    public static final DateHistogramInterval SECOND = DateHistogramInterval.SECOND;

    public ElasticSearchRestConversionStrategy() {

    }

    SearchRequest convertQuery(Query query, QueryOptions options) {
        //LOGGER.debug("Query is " + query + " QueryOptions is " + options);

        def source = createSourceBuilderWithState(query, options);

        if (query.fields && query.fields != SelectClause.ALL_FIELDS) {
            source.fetchSource(query.fields as String[]);
        }

        convertAggregations(query, source);

        SearchRequest request = buildRequest(query, source);
        return request;
    }

    /*
     * Create and return an elastic search SourceBuilder that takes into account the
     * current filter state and selection state. It takes an input query, applies
     * the current filter and selection state associated with this
     * ConversionStrategy to it and returns a SourceBuilder seeded with the
     * resultant query param.
     */

    protected SearchSourceBuilder createSourceBuilderWithState(Query query, QueryOptions options) {
        return createSourceBuilderWithState(query, options, null);
    }

    protected SearchSourceBuilder createSourceBuilderWithState(Query query, QueryOptions options,
            WhereClause extraWhereClause) {

        // dataSet = new DataSet(databaseName: query.databaseName, tableName:
        // query.tableName);

        // Get all the (top level) WhereClauses, from the Filter and query
        List<WhereClause> whereClauses = collectWhereClauses(query, options, extraWhereClause);

        // Convert the WhereClauses into a single ElasticSearch QueryBuilder object
        whereFilter = convertWhereClauses(whereClauses);

        SearchSourceBuilder ssb = createSearchSourceBuilder(query).query(whereFilter);
        return ssb;

        // Was:
        // return
        // createSearchSourceBuilder(query).query(QueryBuilders.filteredQuery(null,
        // whereFilter))
    }

    /**
     * Returns a list of Neon WhereClause objects, some of which might have embedded
     * WhereClauses
     */
    private List<WhereClause> collectWhereClauses(Query query, QueryOptions options, WhereClause extraWhereClause) {
        // Start the list as empty unless an extra WhereClause is passed
        List<WhereClause> whereClauses = extraWhereClause != null ? Arrays.asList(extraWhereClause) : new ArrayList<>();

        // If the Query has a WhereClaus, add it.
        if (query.getFilter() != null && query.getFilter().getWhereClause() != null) {
            whereClauses.add(query.getFilter().getWhereClause());
        }

        // Add the rest of the filters unless told not to.
        if (!options.isIgnoreFilters()) {
            whereClauses.addAll(createWhereClausesForFilters(dataSet, filterState, options.ignoredFilterIds));
        }

        // If selectionOnly, then
        if (options.isSelectionOnly()) {
            whereClauses.addAll(createWhereClausesForFilters(dataSet, selectionState));
        }

        whereClauses.addAll(getCountFieldClauses(query));

        return whereClauses;
    }

    private Collection<? extends WhereClause> getCountFieldClauses(Query query) {
        //TODO:
        return null;
    }

    /**
     * Given a list of WhereClause objects, convert them into a QueryBuilder. In
     * this case, a BoolQueryBuilder that combines all the subsidiary QueryBuilder
     * objects
     */
    protected static QueryBuilder convertWhereClauses(List<WhereClause> whereClauses) {

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        // Build the elasticsearch filters for the where clauses
        List<QueryBuilder> inners = new ArrayList<>();
        whereClauses.forEach(whereClause -> {
            inners.add(convertWhereClause(whereClause));
        });

        return queryBuilder;

    }

    private static QueryBuilder convertWhereClause(WhereClause clause) {

        if (clause instanceof SingularWhereClause) {
            return convertSingularWhereClause(clause);

        } else if (clause instanceof AndWhereClause || clause instanceof OrWhereClause) {
            return convertCompoundWhereClause(clause);
        } else {

            throw new RuntimeErrorException(null, "Unknown where clause: " + clause.getClass());
            // throw new NeonConnectionException("Unknown where clause:
            // ${clause.getClass()}")
        }

    }

    private static QueryBuilder convertCompoundWhereClause(WhereClause clause) {

        // TODO:
        return null;
    }

    private static QueryBuilder convertSingularWhereClause(WhereClause clause) {

        // TODO:
        return null;
    }

    public static boolean isCountFieldAggregation(AggregateClause clause) {
        return clause != null && clause.getOperation() == "count" && clause.getField() != "*";
    }

    public static boolean isCountAllAggregation(AggregateClause clause) {
        return clause != null && clause.getOperation() == "count" && clause.getField() == "*";
    }

    public static int getOffset(Query query) {
        if (query != null && query.getOffsetClause() != null) {
            return query.getOffsetClause().getOffset();
        } else {
            return 0;
        }
    }

    public static int getLimit(Query query) {
        return getLimit(query, false);
    }

    public static int getLimit(Query query, boolean supportsUnlimited) {
        if (query != null && query.getLimitClause() != null) {
            int limitClauselimit = query.getLimitClause().getLimit();
            if (supportsUnlimited) {
                return limitClauselimit;
            }
            if (limitClauselimit < RESULT_LIMIT) {
                return limitClauselimit;
            }
            return RESULT_LIMIT;
        }

        if (supportsUnlimited) {
            return 0;
        }

        // Set the limit to the max (10,000) minus the offset. This is so the 'window'
        // of returned results
        // is less than the max. See:
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html
        return Math.max(RESULT_LIMIT - getOffset(query), 0);
    }

}