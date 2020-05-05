package com.ncc.neon.adapters.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.ncc.neon.models.queries.AggregateByFieldClause;
import com.ncc.neon.models.queries.AggregateByTotalCountClause;
import com.ncc.neon.models.queries.AndWhereClause;
import com.ncc.neon.models.queries.CompoundWhereClause;
import com.ncc.neon.models.queries.GroupByClause;
import com.ncc.neon.models.queries.GroupByFieldClause;
import com.ncc.neon.models.queries.GroupByOperationClause;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.queries.OrWhereClause;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.SelectClause;
import com.ncc.neon.models.queries.SingularWhereClause;
import com.ncc.neon.models.queries.OrderByClause;
import com.ncc.neon.models.queries.OrderByFieldClause;
import com.ncc.neon.models.queries.OrderByOperationClause;
import com.ncc.neon.models.queries.Order;
import com.ncc.neon.models.queries.WhereClause;
import com.ncc.neon.util.DateUtil;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
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

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchQueryConverter {
    static final String[] DATE_OPERATIONS = { "year", "month", "dayOfMonth", "dayOfWeek", "hour", "minute", "second" };
    static final int RESULT_LIMIT = 10000;
    static final String STATS_AGG_PREFIX = "_statsFor_";
    static final String TERM_PREFIX = "_term";

    public ElasticsearchQueryConverter() {
    }

    private static void logObject(String name, Object object) {
        log.debug(name + ":  " + object.toString());
    }

    public static SearchRequest convertQuery(Query query) {
        SearchSourceBuilder source = createSourceBuilderWithState(query);

        String[] includes = collectFields(query).toArray(new String[] {});
        if (includes.length > 0) {
            source.fetchSource(includes, null);
        }

        convertAggregations(query, source);

        SearchRequest request = createSearchRequest(source, query);
        return request;
    }

    public static List<String> collectFields(Query query) {
        return query.getSelectClause().getFieldClauses().stream()
            .filter(fieldClause -> fieldClause.getDatabase().equals(query.getSelectClause().getDatabase()) &&
                fieldClause.getTable().equals(query.getSelectClause().getTable()))
            .map(fieldClause -> fieldClause.getField()).distinct().collect(Collectors.toList());
    }

    /*
     * Create and return an elastic search SourceBuilder that takes into account the
     * current filter state and selection state. It takes an input query, applies
     * the current filter and selection state associated with this
     * ConversionStrategy to it and returns a SourceBuilder seeded with the
     * resultant query param.
     */
    private static SearchSourceBuilder createSourceBuilderWithState(Query query) {
        return createSourceBuilderWithState(query, null);
    }

    private static SearchSourceBuilder createSourceBuilderWithState(Query query, WhereClause extraWhereClause) {
        // Get all the (top level) WhereClauses, from the Filter and query
        List<WhereClause> whereClauses = collectWhereClauses(query, extraWhereClause);

        SearchSourceBuilder ssb = createSearchSourceBuilder(query);

        // Convert the WhereClauses into a single ElasticSearch QueryBuilder object
        if (whereClauses.size() > 0) {
            QueryBuilder qbWithWhereClauses = convertWhereClauses(query.getSelectClause(), whereClauses);
            if (qbWithWhereClauses != null) {
                ssb = ssb.query(qbWithWhereClauses);
            }
        }

        return ssb;
    }

    /**
     * Returns a list of Neon WhereClause objects, some of which might have embedded
     * WhereClauses
     */
    private static List<WhereClause> collectWhereClauses(Query query, WhereClause extraWhereClause) {
        // Start the list as empty unless an extra WhereClause is passed
        List<WhereClause> whereClauses = extraWhereClause != null ? Arrays.asList(extraWhereClause) : new ArrayList<>();

        // If the Query has a WhereClaus, add it.
        if (query.getWhereClause() != null) {
            whereClauses.add(query.getWhereClause());
        }

        whereClauses.addAll(getCountFieldClauses(query));

        return whereClauses;
    }

    private static Collection<? extends WhereClause> getCountFieldClauses(Query query) {
        if (query.getAggregateClauses() != null) {
            // Do not add an exists filter (!= null) on fields that are group function names since they are not real fields!
            return query.getAggregateClauses().stream().filter(aggClause -> aggClause != null &&
                aggClause instanceof AggregateByFieldClause && aggClause.getOperation().equals("count"))
                // TODO Don't add the new filter (!= null) if it already exists in the query.
                .map(aggClause ->
                    SingularWhereClause.fromNull(((AggregateByFieldClause) aggClause).getFieldClause(), "!=")
                )
                .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    /**
     * Given a list of WhereClause objects, convert them into a QueryBuilder. In
     * this case, a BoolQueryBuilder that combines all the subsidiary QueryBuilder
     * objects
     */
    private static QueryBuilder convertWhereClauses(SelectClause selectClause, List<WhereClause> whereClauses) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        // Build the elasticsearch filters for the where clauses
        List<QueryBuilder> convertedWhereClauses = whereClauses.stream()
            .map(whereClause -> convertWhereClause(selectClause, whereClause))
            .filter(convertedWhereClause -> convertedWhereClause != null).collect(Collectors.toList());

        convertedWhereClauses.forEach(convertedWhereClause -> {
            queryBuilder.must(convertedWhereClause);
        });

        return convertedWhereClauses.size() > 0 ? queryBuilder : null;
    }

    private static QueryBuilder convertWhereClause(SelectClause selectClause, WhereClause whereClause) {
        if (whereClause instanceof SingularWhereClause) {
            return convertSingularWhereClause(selectClause, (SingularWhereClause) whereClause);
        }
        if (whereClause instanceof CompoundWhereClause) {
            return convertCompoundWhereClause(selectClause, (CompoundWhereClause) whereClause);
        }
        return null;
    }

    private static QueryBuilder convertCompoundWhereClause(SelectClause selectClause, CompoundWhereClause whereClause) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        if (whereClause instanceof AndWhereClause) {
            whereClause.getWhereClauses().stream().map(innerWhere -> convertWhereClause(selectClause, innerWhere))
                .filter(inner -> inner != null).forEach(inner -> {
                    queryBuilder.must(inner);
                });
        }
        
        if (whereClause instanceof OrWhereClause) {
            whereClause.getWhereClauses().stream().map(innerWhere -> convertWhereClause(selectClause, innerWhere))
                .filter(inner -> inner != null).forEach(inner -> {
                    queryBuilder.should(inner);
                });
        }

        return queryBuilder;
    }

    private static QueryBuilder convertSingularWhereClause(SelectClause selectClause, SingularWhereClause clause) {
        if (!clause.getLhs().getDatabase().equals(selectClause.getDatabase()) ||
            !clause.getLhs().getTable().equals(selectClause.getTable())) {
            return null;
        }

        if (Arrays.asList("<", ">", "<=", ">=").contains(clause.getOperator())) {
            Object rhs = clause.isDate() ? DateUtil.transformDateToString(clause.getRhsDate()) : clause.getRhs();

            UnaryOperator<RangeQueryBuilder> outputRqb = inputRqb -> {
                switch (clause.getOperator()) {
                    case "<": return inputRqb.lt(rhs);
                    case ">": return inputRqb.gt(rhs);
                    case "<=": return inputRqb.lte(rhs);
                    case ">=": return inputRqb.gte(rhs);
                    default: return inputRqb;
                }
            };

            return outputRqb.apply(QueryBuilders.rangeQuery(clause.getLhs().getField()));
        };

        if (Arrays.asList("contains", "not contains", "notcontains").contains(clause.getOperator())) {
            RegexpQueryBuilder regexFilter = QueryBuilders.regexpQuery(clause.getLhs().getField(), ".*" +
                clause.getRhs() + ".*");
            return clause.getOperator().equals("contains") ? regexFilter : QueryBuilders.boolQuery()
                .mustNot(regexFilter);
        };

        if (Arrays.asList("=", "!=").contains(clause.getOperator())) {
            boolean hasValue = !(clause.isNull());

            // Do not create a NOT EQUALS filter on the _id field and an empty string or else ES will throw an error.
            if (clause.getLhs().getField().equals("_id") && clause.isString() && clause.getRhsString().equals("")) {
                return null;
            }

            QueryBuilder filter = hasValue ? QueryBuilders.termQuery(clause.getLhs().getField(),
                clause.isDate() ? DateUtil.transformDateToString(clause.getRhsDate()) : clause.getRhs()) :
                QueryBuilders.existsQuery(clause.getLhs().getField());

            return (clause.getOperator().equals("!=") == !hasValue) ? filter : QueryBuilders.boolQuery()
                .mustNot(filter);
        };

        // TODO Should the "in" and "notin" operators be deprecated?
        if (Arrays.asList("in", "notin").contains(clause.getOperator())) {
            TermsQueryBuilder filter = QueryBuilders.termsQuery(clause.getLhs().getField(),
                Arrays.asList(clause.getRhs()));
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
        List<String> fields = collectFields(query);

        if (query.isDistinct()) {
            if (fields.size() == 0 || fields.size() > 1) {
                throw new RuntimeException("Distinct call requires one field");
            }

            TermsAggregationBuilder termsAggregations = AggregationBuilders.terms("distinct")
                .field(fields.get(0)).size(getLimit(query));
            source.aggregation(termsAggregations);
        } else {
            convertMetricAggregations(query, source);
        }
    }

    private static void convertMetricAggregations(Query query, SearchSourceBuilder source) {
        List<StatsAggregationBuilder> metricAggregations = getMetricAggregations(query);

        if (query.getGroupByClauses() != null && !query.getGroupByClauses().isEmpty()) {
            List<AggregationBuilder> bucketAggs = query.getGroupByClauses().stream().map(clause ->
                convertGroupByClause(query, clause)).filter(group -> group != null).collect(Collectors.toList());

            AggregationBuilder lastBucketAgg = bucketAggs.get(bucketAggs.size() - 1);

            metricAggregations.forEach(metricAggregation -> {
                lastBucketAgg.subAggregation(metricAggregation);
            });

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

            if (query.getOrderByClauses() != null) {
                query.getOrderByClauses().stream().map(ElasticsearchQueryConverter::convertSortClause).forEach(clause -> {
                    source.sort(clause);
                });
            }
        }
    }

    private static List<StatsAggregationBuilder> getMetricAggregations(Query query) {
        if (query.getAggregateClauses() != null) {
            return query.getAggregateClauses().stream().filter(aggClause ->
                aggClause instanceof AggregateByFieldClause && !aggClause.getOperation().equals("count"))
                .map(aggClause -> ((AggregateByFieldClause) aggClause).getField())
                .filter(aggField -> aggField != null).distinct().map(aggField ->
                    AggregationBuilders.stats(ElasticsearchQueryConverter.STATS_AGG_PREFIX + aggField).field(aggField))
                .collect(Collectors.toList());
        }

        return new ArrayList<>();
    }

    private static SortBuilder<FieldSortBuilder> convertSortClause(OrderByClause orderClause) {
        return SortBuilders.fieldSort(orderClause.getFieldOrOperation())
            .order(orderClause.getOrder() == Order.ASCENDING ? SortOrder.ASC : SortOrder.DESC);
    }
 
    private static SearchSourceBuilder createSearchSourceBuilder(Query query) {
        int offset = getOffset(query);
        int size = getLimit(query);
        int terminateAfter = size;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().explain(false).from(offset).size(size);
        if (query != null && query.getLimitClause() != null) {
            boolean totalCount = query.getAggregateClauses() == null ? false : query.getAggregateClauses().stream()
                .anyMatch(aggClause -> aggClause instanceof AggregateByTotalCountClause);
            // Don't add terminateAfter to "total count" queries, or else the query won't return the real total count!
            if (!totalCount) {
                return searchSourceBuilder.terminateAfter(query.getLimitClause().getLimit());
            }
        }
        return searchSourceBuilder;
    }

    public static int getOffset(Query query) {
        if (query != null && query.getOffsetClause() != null) {
            return query.getOffsetClause().getOffset();
        } else {
            return 0;
        }
    }

    private static int getLimit(Query query) {
        return getLimit(query, false);
    }

    private static SearchRequest createSearchRequest(SearchSourceBuilder source, Query params) {
        SearchRequest req = new SearchRequest();

        // NOTE:  IN version 5, count was replaced by Query_Then_Fetch with a size=0
        // See: https://www.elastic.co/guide/en/elasticsearch/reference/2.3/search-request-search-type.html
        // req.searchType((params != null && params.getAggregateClauses().size() > 0) ? SearchType.COUNT : SearchType.DFS_QUERY_THEN_FETCH);
        //
        // TODO:  Set size=0 (i.e. limit clause) when type is counts
        String indexName = params.getSelectClause().getDatabase() != null ? params.getSelectClause().getDatabase() :
            "_all";

        String indexType = params.getSelectClause().getTable() != null ? params.getSelectClause().getTable() : "_all";

        req.searchType(SearchType.DFS_QUERY_THEN_FETCH)
                .source(source)
                .indices(indexName)
                .types(indexType);

        if (req.searchType() == SearchType.DFS_QUERY_THEN_FETCH
            && params.getLimitClause() != null && params.getLimitClause().getLimit() > RESULT_LIMIT) {
            req = req.scroll(TimeValue.timeValueMinutes(1));
        }
        return req;
    }

    public static int getLimit(Query query, boolean supportsUnlimited) {
        if (query != null && query.getLimitClause() != null) {
            int limit = query.getLimitClause().getLimit();
            if (supportsUnlimited) {
                return limit;
            }
            if (limit < RESULT_LIMIT && limit > 0) {
                return limit;
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

    private static DateHistogramAggregationBuilder createDateHistAggBuilder(Query query,
        GroupByOperationClause groupByFunctionClause, DateHistogramInterval interval, String format) {

        DateHistogramAggregationBuilder dateHist = AggregationBuilders.dateHistogram(groupByFunctionClause.getLabel())
            .field(groupByFunctionClause.getFieldClause().getField()).dateHistogramInterval(interval).format(format);

        // TODO: is this needed?
        if (groupByFunctionClause.getOperation().equals("dayOfWeek")) {
            dateHist.offset("1d");
        }

        OrderByOperationClause orderByGroupClause = query.getOrderByClauses().stream().filter(
            orderByClause -> orderByClause instanceof OrderByOperationClause &&
            groupByFunctionClause.getLabel().equals(orderByClause.getFieldOrOperation())
        ).map(orderClause -> (OrderByOperationClause) orderClause).findFirst().orElse(null);

        if(orderByGroupClause != null) {
            dateHist.order(BucketOrder.key(orderByGroupClause.getOrder().getDirection() ==
                Order.ASCENDING.getDirection()));
        }

        return dateHist;
    }

    private static AggregationBuilder convertGroupByClause(Query query, GroupByClause groupClause) {
        if (groupClause instanceof GroupByFieldClause) {
            GroupByFieldClause groupByFieldClause = (GroupByFieldClause) groupClause;

            TermsAggregationBuilder termsAggBuilder = AggregationBuilders
                .terms(groupByFieldClause.getField())
                .field(groupByFieldClause.getField()).size(getLimit(query));

            OrderByFieldClause orderByFieldClause = query.getOrderByClauses().stream().filter(
                orderByClause -> orderByClause instanceof OrderByFieldClause &&
                groupByFieldClause.getField().equals(orderByClause.getFieldOrOperation())
            ).map(orderClause -> (OrderByFieldClause) orderClause).findFirst().orElse(null);

            if (orderByFieldClause != null) {
                termsAggBuilder.order(BucketOrder.key(
                    orderByFieldClause.getOrder().getDirection() == Order.ASCENDING.getDirection()));
            }

            return termsAggBuilder;
        }

        if (groupClause instanceof GroupByOperationClause) {
            GroupByOperationClause funcClause = (GroupByOperationClause) groupClause;
            if (Arrays.asList(DATE_OPERATIONS).contains(funcClause.getOperation())) {
                switch (funcClause.getOperation()) {
                    case "year": return createDateHistAggBuilder(query, funcClause, DateHistogramInterval.YEAR, "yyyy");
                    case "month": return createDateHistAggBuilder(query, funcClause, DateHistogramInterval.MONTH, "M");
                    case "dayOfMonth": return createDateHistAggBuilder(query, funcClause, DateHistogramInterval.DAY, "d");
                    case "dayOfWeek": return createDateHistAggBuilder(query, funcClause, DateHistogramInterval.DAY, "e");
                    case "hour": return createDateHistAggBuilder(query, funcClause, DateHistogramInterval.HOUR, "H");
                    case "minute": return createDateHistAggBuilder(query, funcClause, DateHistogramInterval.MINUTE, "m");
                    case "second": return createDateHistAggBuilder(query, funcClause, DateHistogramInterval.SECOND, "s");
                }
            }
        }

        return null;
    }

    public static UpdateRequest convertMutationByIdQuery(MutateQuery mutateQuery) {
        return new UpdateRequest(mutateQuery.getDatabaseName(), mutateQuery.getTableName(), mutateQuery.getDataId())
            .doc(mutateQuery.getFieldsWithValues());
    }
}
