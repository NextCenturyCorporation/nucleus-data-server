package com.ncc.neon.server.services.adapter.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.AndWhereClause;
import com.ncc.neon.server.models.query.clauses.FieldFunction;
import com.ncc.neon.server.models.query.clauses.GroupByClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.GroupByFunctionClause;
import com.ncc.neon.server.models.query.clauses.OrWhereClause;
import com.ncc.neon.server.models.query.clauses.SelectClause;
import com.ncc.neon.server.models.query.clauses.SingularWhereClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.clauses.WhereClause;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
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

        if(query.getAggregates() != null) {
            query.getAggregates().forEach(aggClause -> {
                if (isCountFieldAggregation(aggClause)) {
                    clauses.add(SingularWhereClause.fromNull(aggClause.getField(), "!="));
                }
            });
        }

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
            return clause.getOperator().equals("contains") ? regexFilter : QueryBuilders.boolQuery().mustNot(regexFilter);
        };

        if(Arrays.asList("=", "!=").contains(clause.getOperator())) {
            boolean hasValue = !(clause.isNull());

            QueryBuilder filter = hasValue ?
                QueryBuilders.termQuery(clause.getLhs(), Arrays.asList(clause.getRhs())) :
                QueryBuilders.existsQuery(clause.getLhs());

            return (clause.getOperator().equals("!=") == !hasValue) ? filter : QueryBuilders.boolQuery().mustNot(filter);
        };

        if(Arrays.asList("in", "notin").contains(clause.getOperator())) {
            TermsQueryBuilder filter = QueryBuilders.termsQuery(clause.getLhs(), Arrays.asList(clause.getRhs()));
            return (clause.getOperator().equals("in")) ? filter : QueryBuilders.boolQuery().mustNot(filter);
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
            if (query.getFields() == null || query.getFields().isEmpty() || query.getFields().size() > 1) {
                throw new RuntimeException("Distinct call requires one field");
            }

            TermsAggregationBuilder termsAggregations = AggregationBuilders.terms("distinct").field(query.getFields().get(0)).size(getLimit(query));
            source.aggregation(termsAggregations);
        } else {
            convertMetricAggregations(query, source);
        }
    }

    // TODO: do we need this?
    // use this for getAggregates() (AggregateClause extends FieldFunction)
    private static SortClause findMatchingSortClause(Query query, FieldFunction matchClause) {
        return query.getSortClauses().stream().filter(sc -> {
            return matchClause.getName().equals(sc.getFieldName());
        }).findFirst().orElse(null);
    }

    private static SortClause findMatchingSortClause(Query query, GroupByFieldClause matchClause) {
        return query.getSortClauses().stream().filter(sc -> {
            return matchClause.getField().equals(sc.getFieldName());
        }).findFirst().orElse(null);
    }

    private static void convertMetricAggregations(Query query, SearchSourceBuilder source) {
        List<StatsAggregationBuilder> metricAggregations = getMetricAggregations(query);

        if (query.getGroupByClauses() != null && !query.getGroupByClauses().isEmpty()) {
            List<AggregationBuilder> bucketAggs = query.getGroupByClauses().stream().map(clause -> {
                return convertGroupByClause(query, clause);
            }).collect(Collectors.toList());

            AggregationBuilder lastBucketAgg = bucketAggs.get(bucketAggs.size() - 1);

            metricAggregations.forEach(metricAggregation -> {
                lastBucketAgg.subAggregation(metricAggregation);
            });

            // copy + pasting commented out Groovy code in case its needed in the future
//            query.aggregates.each { ac ->
//                def sc = findMatchingSortClause(query, ac)
//                if (sc) {
//                    def sortOrder = sc.sortOrder == com.ncc.neon.query.clauses.SortOrder.ASCENDING
//                    bucketAggregations.each { bucketAgg ->
//                        if (!(bucketAgg instanceof DateHistogramBuilder)) {
//                            bucketAgg.order(Terms.Order.aggregation(TERM_PREFIX, sortOrder))
//                        }
//                    }
//                    def lastAgg = bucketAggregations.last()
//                    if (!(lastAgg instanceof DateHistogramBuilder)) {
//                        def aggOrder
//                        if (isCountAllAggregation(ac)) {
//                            aggOrder = Terms.Order.count(sortOrder)
//                        } else {
//                            aggOrder = Terms.Order.aggregation("${STATS_AGG_PREFIX}${ac.field}" as String, ac.operation as String, sortOrder)
//                        }
//                        lastAgg.order(aggOrder)
//                    }
//                }
//            }

            // on each aggregation, except the last - nest the next aggregation
            for (int index = 0; index < bucketAggs.size() - 1; index++) {
                AggregationBuilder bucketAgg = bucketAggs.get(index);
                bucketAgg.subAggregation(bucketAggs.get(index + 1));
                bucketAggs.set(index, bucketAgg);
            }

            source.aggregation(bucketAggs.get(0));

        } else {
            // if there are no groupByClauses, apply sort and metricAggregations directly to source
            metricAggregations.forEach(metricAgg -> {
                source.aggregation(metricAgg);
            });

            if(query.getSortClauses() != null) {
                query.getSortClauses().stream().map(ElasticSearchRestConversionStrategy::convertSortClause).forEach(clause -> {
                    source.sort(clause);
                });
            }
        }
    }

    private static List<StatsAggregationBuilder> getMetricAggregations(Query query) {
        List<StatsAggregationBuilder> aggregates = new ArrayList<StatsAggregationBuilder>();

        if(query.getAggregates() != null) {
            aggregates = query.getAggregates().stream()
            .filter(clause -> {
                return !isCountAllAggregation(clause) && !isCountFieldAggregation(clause);
            }).map(agg -> {
                return agg.getField();
            }).distinct().map(agg -> {
                return AggregationBuilders.stats(ElasticSearchRestConversionStrategy.STATS_AGG_PREFIX + agg).field(agg);
            }).collect(Collectors.toList());
        }

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
        return clause != null && clause.getOperation().equals("count") && !clause.getField().equals("*");
    }

    public static boolean isCountAllAggregation(AggregateClause clause) {
        return clause != null && clause.getOperation().equals("count") && clause.getField().equals("*");
    }

    private static SortBuilder<FieldSortBuilder> convertSortClause(SortClause clause) {
        SortOrder order = clause.getSortOrder() == com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING ?
            SortOrder.ASC : SortOrder.DESC;
        return SortBuilders.fieldSort(clause.getFieldName()).order(order);
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
        // req.searchType((params != null && params.getAggregates().size() > 0) ? SearchType.COUNT : SearchType.DFS_QUERY_THEN_FETCH);
        //
        // TODO:  Set size=0 (i.e. limit clause) when type is counts
        String indicesValue = 
            (params != null && params.getFilter() != null && params.getFilter().getDatabaseName() != null)
            ? params.getFilter().getDatabaseName() : "_all";

        String typesValue = 
            (params != null && params.getFilter() != null && params.getFilter().getTableName() != null)
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


    // TODO: 964: would there be another case where the other findMatchingSortClause()
    // for FieldFunction would be needed? - not sure there is currently

    // Used by convertGroupByClause method
    private static DateHistogramAggregationBuilder createDateHistAggBuilder(GroupByFunctionClause clause,
        DateHistogramInterval interval, String format) {

        DateHistogramAggregationBuilder dateHist = AggregationBuilders.dateHistogram(clause.getName())
            .field(clause.getField()).dateHistogramInterval(interval).format(format);

        // TODO: is this needed?
        if(clause.getOperation().equals("dayOfWeek")) {
            dateHist.offset("1d");
        }

        return dateHist;
    }

    private static AggregationBuilder convertGroupByClause(Query query, GroupByClause clause) {
        if(clause instanceof GroupByFieldClause) {
            GroupByFieldClause fieldClause = (GroupByFieldClause) clause;
            TermsAggregationBuilder termsAggBuilder = AggregationBuilders.terms(fieldClause.getField()).field(fieldClause.getField()).size(getLimit(query));
            SortClause sortClause = findMatchingSortClause(query, fieldClause);

            if(sortClause != null) {
                boolean sortOrder = sortClause.getSortOrder().getDirection() == com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING.getDirection();
                termsAggBuilder.order(BucketOrder.key(sortOrder));
            }

            return termsAggBuilder;
        }

        if(clause instanceof GroupByFunctionClause) {
            GroupByFunctionClause funcClause = (GroupByFunctionClause) clause;
            if(Arrays.asList(DATE_OPERATIONS).contains(funcClause.getOperation())) {
                switch (funcClause.getOperation()) {
                    case "year": return createDateHistAggBuilder(funcClause, DateHistogramInterval.YEAR, "yyyy");
                    case "month": return createDateHistAggBuilder(funcClause, DateHistogramInterval.MONTH, "M");
                    case "dayOfMonth": return createDateHistAggBuilder(funcClause, DateHistogramInterval.DAY, "d");
                    case "dayOfWeek": return createDateHistAggBuilder(funcClause, DateHistogramInterval.DAY, "e");
                    case "hour": return createDateHistAggBuilder(funcClause, DateHistogramInterval.HOUR, "H");
                    case "minute": return createDateHistAggBuilder(funcClause, DateHistogramInterval.MINUTE, "m");
                    case "second": return createDateHistAggBuilder(funcClause, DateHistogramInterval.SECOND, "s");
                }
            }
        }

        throw new RuntimeException("Unknown groupByClause: " + clause.getClass());
    }
}