package com.ncc.neon.server.adapters.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.AndWhereClause;
import com.ncc.neon.server.models.query.clauses.GroupByClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.GroupByFunctionClause;
import com.ncc.neon.server.models.query.clauses.OrWhereClause;
import com.ncc.neon.server.models.query.clauses.SelectClause;
import com.ncc.neon.server.models.query.clauses.SingularWhereClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.clauses.WhereClause;
import com.ncc.neon.server.models.query.result.TabularQueryResult;
import com.ncc.neon.util.DateUtil;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchTransformer {
    static final String[] DATE_OPERATIONS = { "year", "month", "dayOfMonth", "dayOfWeek", "hour", "minute", "second" };
    static final int RESULT_LIMIT = 10000;
    static final String STATS_AGG_PREFIX = "_statsFor_";
    static final String TERM_PREFIX = "_term";

    /*
     * A small class to hold the important information about an aggregation bucket
     * between when the buckets are taken out of ElasticSearch's hierarchical
     * arrangement and when everything can be extracted into the format that the
     * Neon API uses.
     */
    @Data
    private static class TransformedAggregationBucket {
        private Map<String, Aggregation> aggregatedValues = new LinkedHashMap<>();
        private Map<String, TransformedAggregationData> groupByKeys = new LinkedHashMap<>();
        // TODO Deprecated
        private long docCount;
    }

    @AllArgsConstructor
    @Data
    private static class TransformedAggregationData {
        private long count;
        private Object value;
    }

    public ElasticsearchTransformer() {
    }

    private static void logObject(String name, Object object) {
        log.debug(name + ":  " + object.toString());
    }

    public static SearchRequest transformQuery(Query query, QueryOptions options) {
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
    private static SearchSourceBuilder createSourceBuilderWithState(Query query, QueryOptions options) {
        return createSourceBuilderWithState(query, options, null);
    }

    private static SearchSourceBuilder createSourceBuilderWithState(Query query, QueryOptions options,
            WhereClause extraWhereClause) {

        // Get all the (top level) WhereClauses, from the Filter and query
        List<WhereClause> whereClauses = collectWhereClauses(query, options, extraWhereClause);

        SearchSourceBuilder ssb = createSearchSourceBuilder(query);

        // Convert the WhereClauses into a single ElasticSearch QueryBuilder object
        if(whereClauses.size() > 0) {
            QueryBuilder qbWithWhereClauses = convertWhereClauses(whereClauses);
            ssb = ssb.query(qbWithWhereClauses);
        }

        return ssb;
    }

    /**
     * Returns a list of Neon WhereClause objects, some of which might have embedded
     * WhereClauses
     */
    private static List<WhereClause> collectWhereClauses(Query query, QueryOptions options, WhereClause extraWhereClause) {
        // Start the list as empty unless an extra WhereClause is passed
        List<WhereClause> whereClauses = extraWhereClause != null ? Arrays.asList(extraWhereClause) : new ArrayList<>();

        // If the Query has a WhereClaus, add it.
        if (query.getFilter() != null && query.getFilter().getWhereClause() != null) {
            whereClauses.add(query.getFilter().getWhereClause());
        }

        whereClauses.addAll(getCountFieldClauses(query));

        return whereClauses;
    }

    private static Collection<? extends WhereClause> getCountFieldClauses(Query query) {
        List<SingularWhereClause> clauses = new ArrayList<>();

        if(query.getAggregates() != null) {
            // Do not add an exists filter (!= null) on fields that are group function names since they are not real fields!
            Map<String, Boolean> groupNames = query.getGroupByClauses().stream().filter(clause -> clause instanceof GroupByFunctionClause)
                .map(clause -> ((GroupByFunctionClause) clause).getName()).collect(Collectors.toMap(name -> name, name -> true));
            query.getAggregates().forEach(aggClause -> {
                // TODO Don't add the new filter (!= null) if it already exists in the query.
                if (isCountFieldAggregation(aggClause) && !groupNames.containsKey(aggClause.getField())) {
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
    private static QueryBuilder convertWhereClauses(List<WhereClause> whereClauses) {
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

            return outputRqb.apply(QueryBuilders.rangeQuery(clause.getLhs()));
        };

        if(Arrays.asList("contains", "not contains", "notcontains").contains(clause.getOperator())) {
            RegexpQueryBuilder regexFilter = QueryBuilders.regexpQuery(clause.getLhs(), ".*" + clause.getRhs() + ".*");
            return clause.getOperator().equals("contains") ? regexFilter : QueryBuilders.boolQuery().mustNot(regexFilter);
        };

        if(Arrays.asList("=", "!=").contains(clause.getOperator())) {
            boolean hasValue = !(clause.isNull());

            QueryBuilder filter = hasValue ? QueryBuilders.termQuery(clause.getLhs(),
                clause.isDate() ? DateUtil.transformDateToString(clause.getRhsDate()) : clause.getRhs()) :
                QueryBuilders.existsQuery(clause.getLhs());

            return (clause.getOperator().equals("!=") == !hasValue) ? filter : QueryBuilders.boolQuery().mustNot(filter);
        };

        // TODO Should the "in" and "notin" operators be deprecated?
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

    // Previously this method took either a GroupByFieldClause or FieldFunction clause. It does
    // not currently look like the FieldFunction case is being used, so that was removed.
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
                query.getSortClauses().stream().map(ElasticsearchTransformer::convertSortClause).forEach(clause -> {
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
                return !isTotalCountAggregation(clause) && !isCountFieldAggregation(clause);
            }).map(agg -> {
                return agg.getField();
            }).distinct().map(agg -> {
                return AggregationBuilders.stats(ElasticsearchTransformer.STATS_AGG_PREFIX + agg).field(agg);
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

    private static boolean isCountFieldAggregation(AggregateClause clause) {
        return clause != null && clause.getOperation().equals("count") && !clause.getField().equals("*");
    }

    private static boolean isTotalCountAggregation(AggregateClause clause) {
        return clause != null && clause.getOperation().equals("count") && clause.getField().equals("*");
    }

    private static SortBuilder<FieldSortBuilder> convertSortClause(SortClause clause) {
        SortOrder order = clause.getSortOrder() == com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING ?
            SortOrder.ASC : SortOrder.DESC;
        return SortBuilders.fieldSort(clause.getFieldName()).order(order);
    }
 
    private static SearchSourceBuilder createSearchSourceBuilder(Query query) {
        int offset = getOffset(query);
        int size = getLimit(query);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .explain(false).from(offset).size(size);
        return searchSourceBuilder;
    }

    private static int getOffset(Query query) {
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
            && params.getLimitClause() != null && params.getLimitClause().getLimit() > RESULT_LIMIT) {
            req = req.scroll(TimeValue.timeValueMinutes(1));
        }
        return req;
    }

    private static int getLimit(Query query, boolean supportsUnlimited) {
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

    public static TabularQueryResult transformResults(Query query, QueryOptions options, SearchResponse response) {
        List<AggregateClause> aggregateClauses = query.getAggregates();
        List<GroupByClause> groupByClauses = query.getGroupByClauses();

        Aggregations aggregationResults = response.getAggregations();

        TabularQueryResult results;

        if (aggregateClauses.size() > 0 && groupByClauses.size() == 0) {
            Map<String, Object> metrics = extractMetrics(aggregateClauses, aggregationResults != null ? aggregationResults.asMap() : null,
                response.getHits().getTotalHits());
            results = new TabularQueryResult(List.of(metrics));
        } else if (aggregateClauses.size() > 0 && groupByClauses.size() > 0) {
            List<TransformedAggregationBucket> buckets = extractBuckets(groupByClauses,
                (MultiBucketsAggregation) aggregationResults.asList().get(0));
            buckets = combineDuplicateBuckets(buckets);
            List<Map<String, Object>> extractedMetrics = extractMetricsFromBuckets(aggregateClauses, buckets, response.getHits().getTotalHits());
            extractedMetrics = sortBuckets(query.getSortClauses(), extractedMetrics);
            extractedMetrics = limitBuckets(extractedMetrics, query);
            results = new TabularQueryResult(extractedMetrics);
        } else if (query.isDistinct()) {
            results = new TabularQueryResult(extractDistinct(query, (MultiBucketsAggregation) aggregationResults.asList().get(0)));
        } else {
            results = new TabularQueryResult(extractHitsFromResults(response));
        }

        return results;
    }

    private static List<Map<String, Object>> extractHitsFromResults(SearchResponse response) {
        return Arrays.stream(response.getHits().getHits()).map(searchHit -> {
            // Copy the map since it may be immutable.
            Map<String, Object> map = searchHit.getSourceAsMap().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue));
            map.put("_id", searchHit.getId());
            return map;
        }).collect(Collectors.toList());

    }

    private static List<Map<String, Object>> extractDistinct(Query query, MultiBucketsAggregation aggResult) {
        String field = query.getFields().get(0);

        final List<Map<String, Object>> unsortedDistinctValues = new ArrayList<>();
        aggResult.getBuckets().forEach(a -> {
            Map<String, Object> accumulator = new LinkedHashMap<>();
            accumulator.put(field, a.getKey());
            unsortedDistinctValues.add(accumulator);
        });

        List<Map<String, Object>> distinctValues = sortDistinct(query, unsortedDistinctValues);

        int offset = getOffset(query);
        int limit = getLimit(query, true);

        if (limit == 0) {
            limit = distinctValues.size();
        }

        int endIndex = Math.max(Math.min((limit + offset), distinctValues.size()), offset);

        distinctValues = ((offset >= distinctValues.size()) ? new ArrayList<>() : distinctValues.subList(offset, endIndex));

        return distinctValues;
    }

    private static List<Map<String, Object>> sortDistinct(Query query, List<Map<String, Object>> values) {
        String field = query.getFields().get(0);

        List<SortClause> sorts = query.getSortClauses().stream().filter(sort -> sort.getFieldName() == field)
            .collect(Collectors.toList());

        if (sorts.size() > 0) {
            SortClause sort = sorts.get(0);
            values.sort(new Comparator<Map<String, Object>>() {
                @Override
                @SuppressWarnings({"rawtypes", "unchecked"})
                public int compare(Map<String, Object> a, Map<String, Object> b) {
                    if(a instanceof Comparable && b instanceof Comparable) {
                        return sort.getSortDirection() * ((Comparable) a.get(field)).compareTo(((Comparable) b.get(field)));
                    }
                    if(isNumeric(a.get(field).toString()) && isNumeric(b.get(field).toString())) {
                        return sort.getSortDirection() * (Double.valueOf(a.get(field).toString())).compareTo(
                            Double.valueOf(b.get(field).toString()));
                    }
                    return sort.getSortDirection() * a.get(field).toString().compareTo(b.get(field).toString());
                }
            });
        }
        return values;
    }

    private static Map<String, Object> extractMetrics(List<AggregateClause> clauses, Map<String, Aggregation> results,
            long totalCount) {
        return extractMetrics(clauses, results, new LinkedHashMap<>(), totalCount);
    }

    private static Map<String, Object> extractMetrics(List<AggregateClause> clauses, Map<String, Aggregation> results,
            Map<String, TransformedAggregationData> groupResults, long totalCount) {

        List<AggregateClause> metricsClauses = clauses.stream().filter(it ->
            !isTotalCountAggregation(it) &&
            !isCountFieldAggregation(it)).collect(Collectors.toList());

        List<AggregateClause> countFieldClauses = clauses.stream().filter(it ->
            isCountFieldAggregation(it)).collect(Collectors.toList());

        List<AggregateClause> totalCountClauses = clauses.stream().filter(it ->
            isTotalCountAggregation(it)).collect(Collectors.toList());

        Map<String, Object> metrics = metricsClauses.stream().collect(Collectors.toMap(clause -> clause.getName(), clause -> {
            Stats result = (Stats) results.get(ElasticsearchTransformer.STATS_AGG_PREFIX + clause.getField());
            Double metric = 0.0;
            switch (clause.getOperation()) {
            case "avg":
                metric = result.getAvg();
                break;
            case "max":
                metric = result.getMax();
                break;
            case "min":
                metric = result.getMin();
                break;
            case "sum":
                metric = result.getSum();
                break;
            }
            return metric;
        }));

        if (!totalCountClauses.isEmpty()) {
            metrics.putAll(totalCountClauses.stream().collect(Collectors.toMap(clause -> clause.getName(), clause -> totalCount)));
        }

        metrics.putAll(countFieldClauses.stream().collect(Collectors.toMap(clause -> clause.getName(), clause -> {
            TransformedAggregationData data = groupResults.get(clause.getField());
            return data != null ? data.getCount() : totalCount;
        })));

        return metrics;
    }

    /**
     * The aggregation results from ES will be a tree of aggregation -> buckets ->
     * aggregation -> buckets -> etc we want to flatten it into a list of buckets
     * where we have one item in the list for each leaf in the tree. Each item is an
     * accumulation of all the buckets along the path to the leaf.
     *
     * We process an aggregation by getting the list of buckets and calling extract
     * again for each of them. For each bucket we copy the current accumulator,
     * since we're branching into more paths in the tree.
     *
     * We process a bucket by getting the current groupByClause and using it to get
     * the value we're interested in from the bucket. Then if there are more
     * groupByClauses, we recurse into extract using the rest of the clauses and the
     * nested aggregation. If there are no more clauses to process, then we've
     * reached the bottom of the tree we add any metric aggregations to the bucket
     * and push it onto the result list.
     */
    private static List<TransformedAggregationBucket> extractBuckets(List<GroupByClause> groupByClauses,
            MultiBucketsAggregation aggregation) {
        return extractBuckets(groupByClauses, aggregation, new LinkedHashMap<>(), new ArrayList<>());

    }

    private static List<TransformedAggregationBucket> extractBuckets(List<GroupByClause> groupByClauses,
            MultiBucketsAggregation aggregation, Map<String, TransformedAggregationData> accumulator,
            List<TransformedAggregationBucket> results) {

        aggregation.getBuckets().forEach(bucket -> {
            Map<String, TransformedAggregationData> newAccumulator = new LinkedHashMap<>();
            newAccumulator.putAll(accumulator);
            extractBucket(groupByClauses, bucket, newAccumulator, results);
        });

        return results;
    }

    public static boolean isNumeric(String inputData) {
        return inputData.matches("[-+]?\\d+(\\.\\d+)?");
    }

    private static void extractBucket(List<GroupByClause> groupByClauses, Bucket bucket,
            Map<String, TransformedAggregationData> accumulator, List<TransformedAggregationBucket> results) {

        GroupByClause currentClause = groupByClauses.get(0);

        if (currentClause instanceof GroupByFieldClause) {
            accumulator.put(((GroupByFieldClause) currentClause).getField(), new TransformedAggregationData(
                bucket.getDocCount(), bucket.getKey()));
        } else if (currentClause instanceof GroupByFunctionClause) {
            // Date groups will return numbers (year=2018, month=12, day=30, etc.)
            String key = bucket.getKeyAsString();

            boolean isDateClause = Arrays.asList(ElasticsearchTransformer.DATE_OPERATIONS)
                    .indexOf(((GroupByFunctionClause) currentClause).getOperation()) >= 0 && isNumeric(key);

            accumulator.put(((GroupByFunctionClause) currentClause).getName(), new TransformedAggregationData(
                    bucket.getDocCount(), isDateClause ? Float.parseFloat(key) : key));
        } else {
            throw new RuntimeException("Bad implementation - ${currentClause.getClass()} is not a valid groupByClause");
        }

        List<GroupByClause> tail = groupByClauses.subList(1, groupByClauses.size());
        if (tail != null && tail.size() > 0) {
            extractBuckets(tail, (MultiBucketsAggregation) bucket.getAggregations().asList().get(0), accumulator, results);
        } else {
            TransformedAggregationBucket transformedAggregationBucket = new TransformedAggregationBucket();
            transformedAggregationBucket.setGroupByKeys(accumulator);

            // TODO Deprecated
            transformedAggregationBucket.setDocCount(bucket.getDocCount());

            Aggregations terminalAggs = bucket.getAggregations();
            if (terminalAggs != null) {
                transformedAggregationBucket.getAggregatedValues().putAll(terminalAggs.asMap());
            }
            results.add(transformedAggregationBucket);
        }
    }

    private static List<TransformedAggregationBucket> combineDuplicateBuckets(List<TransformedAggregationBucket> buckets) {
        Map<Map<String, TransformedAggregationData>, TransformedAggregationBucket> mappedBuckets = new LinkedHashMap<>();
        // Iterate over all of the buckets, looking for any that have the same
        // groupByKeys

        buckets.forEach(bucket -> {
            // Only process a bucket if there are documents in it, since we're using a
            // histogram to
            // replicate group-by functionality.
            if (bucket.getDocCount() > 0) {
                TransformedAggregationBucket existingBucket = mappedBuckets.get(bucket.getGroupByKeys());
                if (existingBucket != null) {

                    // If we've already found a bucket with these groupByKeys, then combine them
                    bucket.getAggregatedValues().forEach((key, value) -> {
                        Aggregation existingAgg = existingBucket.getAggregatedValues().get(key);
                        Stats statsValue = (Stats) value;
                        if (existingAgg != null) {
                            Stats stats = (Stats) existingAgg;
                            InternalStats newAgg = createInternalStats(existingAgg.getName(),
                                    stats.getCount() + statsValue.getCount(), stats.getSum() + statsValue.getSum(),
                                    Math.min(stats.getMin(), statsValue.getMin()),
                                    Math.max(stats.getMax(), statsValue.getMax()), DocValueFormat.RAW);
                            existingBucket.getAggregatedValues().put(key, newAgg);
                        } else {
                            throw new RuntimeException("Unknown error on combine duplicate buckets with key:  " + key);
                            // existingBucket.put(key, value);
                        }
                    });
                    existingBucket.setDocCount(existingBucket.getDocCount() + bucket.getDocCount());
                } else {
                    // If there isn't already a bucket with these groupByKeys, then add it to the
                    // map
                    mappedBuckets.put(bucket.getGroupByKeys(), bucket);
                }
            }
        });

        return mappedBuckets.values().stream().collect(Collectors.toList());

    }

    private static InternalStats createInternalStats(String name, long count, double sum, double min, double max,
            DocValueFormat formatter) {
        return new InternalStats(name, count, sum, min, max, formatter, new ArrayList<PipelineAggregator>(),
                new LinkedHashMap<String, Object>());
    }

    private static List<Map<String, Object>> extractMetricsFromBuckets(List<AggregateClause> clauses,
            List<TransformedAggregationBucket> buckets, long totalCount) {

        return buckets.stream().map(bucket -> {
            Map<String, TransformedAggregationData> groupResults = bucket.getGroupByKeys();
            Map<String, Object> result = groupResults.keySet().stream().collect(Collectors.toMap(key -> key,
                key -> groupResults.get(key).getValue()));
            result.putAll(extractMetrics(clauses, bucket.getAggregatedValues(), groupResults, totalCount));
            return result;
        }).collect(Collectors.toList());
    }

    private static List<Map<String, Object>> sortBuckets(List<SortClause> sortClauses,
            List<Map<String, Object>> buckets) {
        if (sortClauses != null && sortClauses.size() > 0) {
            buckets.sort((a, b) -> {
                for (SortClause sortClause : sortClauses) {
                    Object aField = a.get(sortClause.getFieldName());
                    Object bField = b.get(sortClause.getFieldName());
                    int order = 0;

                    if(isFieldDouble(aField.toString()) && isFieldDouble(bField.toString())) {
                        Double aFieldAsDouble = Double.parseDouble(aField.toString());
                        Double bFieldAsDouble = Double.parseDouble(bField.toString());

                        order = sortClause.getSortDirection() * (aFieldAsDouble.compareTo(bFieldAsDouble));
                    } else {
                        order = sortClause.getSortDirection() * (aField.toString().compareTo(bField.toString()));
                    }

                    if (order != 0) {
                        return order;
                    }
                }
                return 0;
            });
        }

        return buckets;
    }

    private static boolean isFieldDouble(String input) {
        try {
            Double.parseDouble(input);
            return true;
        } catch(Exception e) {
            return false;
        }
    }

    private static List<Map<String, Object>> limitBuckets(List<Map<String, Object>> buckets, Query query) {
        int offset = getOffset(query);
        int limit = getLimit(query, true);

        if (limit == 0) {
            limit = buckets.size();
        }

        int endIndex = Math.max(Math.min((limit + offset), buckets.size()), offset);

        List<Map<String, Object>> result = (offset >= buckets.size()) ? new ArrayList<>() : buckets.subList(offset, endIndex);

        return result;
    }
}
