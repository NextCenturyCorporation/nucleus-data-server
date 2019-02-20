package com.ncc.neon.server.services.adapter.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.management.RuntimeErrorException;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.AndWhereClause;
import com.ncc.neon.server.models.query.clauses.FieldFunction;
import com.ncc.neon.server.models.query.clauses.GroupByClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.OrWhereClause;
import com.ncc.neon.server.models.query.clauses.SelectClause;
import com.ncc.neon.server.models.query.clauses.SingularWhereClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.clauses.WhereClause;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
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

        SearchSourceBuilder source = createSourceBuilderWithState(query, options);

        if (query.getFields() != null && query.getFields() != SelectClause.ALL_FIELDS) {
            String[] includes = query.getFields().toArray(new String[query.getFields().size()]);
            source.fetchSource(includes, null);
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

        // Get all the (top level) WhereClauses, from the Filter and query
        List<WhereClause> whereClauses = collectWhereClauses(query, options, extraWhereClause);

        // Convert the WhereClauses into a single ElasticSearch QueryBuilder object
        QueryBuilder qbWithWhereClauses = convertWhereClauses(whereClauses);

        SearchSourceBuilder ssb = createSearchSourceBuilder(query).query(qbWithWhereClauses);
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

        whereClauses.addAll(getCountFieldClauses(query));

        return whereClauses;
    }

    private Collection<? extends WhereClause> getCountFieldClauses(Query query) {
        List<SingularWhereClause> clauses = new ArrayList<>();

        query.getAggregates().forEach(aggClause -> {
            if (isCountFieldAggregation(aggClause)) {
                clauses.add(SingularWhereClause.fromNull(aggClause.getField(), "!="));
            }
        });

        return clauses;
    }

    /**
     * Given a list of WhereClause objects, convert them into a QueryBuilder. In
     * this case, a BoolQueryBuilder that combines all the subsidiary QueryBuilder
     * objects
     */
    protected static QueryBuilder convertWhereClauses(List<WhereClause> whereClauses) {

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        // Build the elasticsearch filters for the where clauses
        List<QueryBuilder> inners = new ArrayList<QueryBuilder>();
        whereClauses.forEach(whereClause -> {
            inners.add(convertWhereClause(whereClause));
        });

        inners.forEach(inner -> {
            queryBuilder.must(inner);
        });

        return queryBuilder;

    }

    private static QueryBuilder convertWhereClause(WhereClause clause) {

        if (clause instanceof SingularWhereClause) {
            return convertSingularWhereClause((SingularWhereClause) clause);
        } else if (clause instanceof AndWhereClause || clause instanceof OrWhereClause) {
            return convertCompoundWhereClause(clause);
        } else {
            throw new RuntimeException("Unknown where clause: " + clause.getClass());
        }

    }

    private static QueryBuilder convertCompoundWhereClause(WhereClause clause) {
        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        if (clause instanceof AndWhereClause) {
            AndWhereClause andClause = (AndWhereClause) clause;
            andClause.getWhereClauses().stream().map(innerWhere -> {
                return convertWhereClause(innerWhere);
            }).forEach(inner -> {
                qb.must(inner);
            });
        } else if (clause instanceof OrWhereClause) {
            OrWhereClause orClause = (OrWhereClause) clause;
            orClause.getWhereClauses().stream().map(innerWhere -> {
                return convertWhereClause(innerWhere);
            }).forEach(inner -> {
                qb.should(inner);
            });
        } else {
            throw new RuntimeException("Unknown where clause: " + clause.getClass());
        }

        return qb;
    }

    private static QueryBuilder convertSingularWhereClause(SingularWhereClause clause) {
        if(Arrays.asList("<", ">", "<=", ">=").contains(clause.getOperator())) {
            UnaryOperator<RangeQueryBuilder> outputRqb = inputRqb -> {
                switch (clause.getOperator()) {
                    case "<": return inputRqb.lt(clause.getRhs());
                    case ">": return inputRqb.gt(clause.getRhs());
                    case "<=": return inputRqb.lte(clause.getRhs());
                    case ">=": return inputRqb.gte(clause.getRhs());
                    default: return inputRqb;
                }
            };

            return outputRqb.apply(QueryBuilders.rangeQuery(clause.getLhs()));
        };

        if(Arrays.asList("contains", "not contains", "notcontains").contains(clause.getOperator())) {
            RegexpQueryBuilder regexFilter = QueryBuilders.regexpQuery(clause.getLhs(), ".*" + clause.getRhs() + ".*");
            return clause.getOperator() == "contains" ? regexFilter : QueryBuilders.boolQuery().mustNot(regexFilter);
        };

        if(Arrays.asList("=", "!=").contains(clause.getOperator())) {
            boolean hasValue = !(clause.isNull());

            QueryBuilder filter = hasValue ?
                QueryBuilders.termQuery(clause.getLhs(), Arrays.asList(clause.getRhs())) :
                QueryBuilders.existsQuery(clause.getLhs());

            return (clause.getOperator() == "!=") == !hasValue ? filter : QueryBuilders.boolQuery().mustNot(filter);
        };

        if(Arrays.asList("in", "notin").contains(clause.getOperator())) {
            TermsQueryBuilder filter = QueryBuilders.termsQuery(clause.getLhs(), Arrays.asList(clause.getRhs()));
            return (clause.getOperator() == "in") ? filter : QueryBuilders.boolQuery().mustNot(filter);
        };

        throw new RuntimeException(clause.getOperator() + " is an invalid operator for a where clause");
    }

    /**
     * create the metric aggregations by doing a stats aggregation for any field where
     * a calculation is requested - this gives us all of the metrics we could
     * possibly need. Also, don't process the count all clauses here, since
     * that will be available either through the hit count in the results, or as
     * doc_count in the buckets
    */
    private static void convertAggregations(Query query, SearchSourceBuilder source) {
        if (query.isDistinct()) {
            if (query.getFields() == null || query.getFields().size() == 0 || query.getFields().size() > 1) {
                throw new RuntimeException("Distinct call requires one field");
            }

            TermsAggregationBuilder termsAggregations = AggregationBuilders.terms("distinct").field(query.getFields().get(0)).size(getLimit(query));
            source.aggregation(termsAggregations);
        } else {
            // TODO: convertMetricAggregations(query, source);
        }
    }

    private static List<StatsAggregationBuilder> getMetricAggregations(Query query) {
        List<StatsAggregationBuilder> aggregates = new ArrayList<StatsAggregationBuilder>();

        aggregates = query.getAggregates().stream()
            .filter(clause -> {
                return !isCountAllAggregation(clause) && !isCountFieldAggregation(clause);
            }).map(agg -> {
                return agg.getField();
            }).distinct().map(agg -> {
                return AggregationBuilders.stats(ElasticSearchRestConversionStrategy.STATS_AGG_PREFIX + agg).field(agg);
            }).collect(Collectors.toList());

        return aggregates;
    }

    private static SearchRequest buildRequest(Query query, SearchSourceBuilder source) {
        SearchRequest request = createSearchRequest(source, query);

        if (query.getFilter() != null && query.getFilter().getTableName() != null) {
            String[] tableNameAsArray = {query.getFilter().getTableName()};
            request.types(tableNameAsArray);
        }

        return request;
    }

    public static boolean isCountFieldAggregation(AggregateClause clause) {
        return clause != null && clause.getOperation() == "count" && clause.getField() != "*";
    }

    public static boolean isCountAllAggregation(AggregateClause clause) {
        return clause != null && clause.getOperation() == "count" && clause.getField() == "*";
    }

    public static SearchSourceBuilder createSearchSourceBuilder(Query query) {
        int offset = getOffset(query);
        int size = getLimit(query);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .explain(false).from(offset).size(size);
        return searchSourceBuilder;
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

    public static SearchRequest createSearchRequest(SearchSourceBuilder source, Query params) {
        SearchRequest req = new SearchRequest();

        // NOTE:  IN version 5, count was replaced by Query_Then_Fetch with a size=0
        // See: https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-request-search-type.html
        // req.searchType((params?.aggregates) ? SearchType.COUNT : SearchType.DFS_QUERY_THEN_FETCH)
        //
        // TODO:  Set size=0 (i.e. limit clause) when type is counts
        String indicesValue = 
            (params != null && params.getFilter() != null & params.getFilter().getDatabaseName() != null)
            ? params.getFilter().getDatabaseName() : "_all";

        String typesValue = 
            (params != null && params.getFilter() != null & params.getFilter().getTableName() != null)
            ? params.getFilter().getTableName() : "_all";

        req.searchType(SearchType.DFS_QUERY_THEN_FETCH)
                .source(source)
                .indices(indicesValue)
                .types(typesValue);

        if (req.searchType() == SearchType.DFS_QUERY_THEN_FETCH
            && params.getLimitClause() != null && params.getLimitClause().getLimit() > 10000) {
            req = req.scroll(TimeValue.timeValueMinutes(1));
        }
        return req;
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