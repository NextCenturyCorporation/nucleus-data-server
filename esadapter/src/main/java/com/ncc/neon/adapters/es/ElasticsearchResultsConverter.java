package com.ncc.neon.adapters.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.ncc.neon.models.queries.AggregateByFieldClause;
import com.ncc.neon.models.queries.AggregateClause;
import com.ncc.neon.models.queries.AggregateByGroupCountClause;
import com.ncc.neon.models.queries.AggregateByTotalCountClause;
import com.ncc.neon.models.queries.GroupByClause;
import com.ncc.neon.models.queries.GroupByFieldClause;
import com.ncc.neon.models.queries.GroupByOperationClause;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.OrderByClause;
import com.ncc.neon.models.queries.OrderByFieldClause;
import com.ncc.neon.models.results.TabularQueryResult;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchResultsConverter {

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

    public ElasticsearchResultsConverter() {
    }

    private static void logObject(String name, Object object) {
        log.debug(name + ":  " + object.toString());
    }

    public static TabularQueryResult convertResults(Query query, SearchResponse response) {
        List<AggregateClause> aggregateClauses = query.getAggregateClauses();
        List<GroupByClause> groupByClauses = query.getGroupByClauses();

        Aggregations aggregationResults = response.getAggregations();

        TabularQueryResult results;

        if (aggregateClauses.size() > 0 && groupByClauses.size() == 0) {
            Map<String, Object> metrics = extractMetrics(aggregateClauses, aggregationResults != null ? aggregationResults.asMap() : null,
                response.getHits().getTotalHits());
            results = new TabularQueryResult(Arrays.<Map<String, Object>>asList(metrics));
        } else if (aggregateClauses.size() > 0 && groupByClauses.size() > 0) {
            List<TransformedAggregationBucket> buckets = extractBuckets(groupByClauses,
                (MultiBucketsAggregation) aggregationResults.asList().get(0));
            buckets = combineDuplicateBuckets(buckets);
            List<Map<String, Object>> extractedMetrics = extractMetricsFromBuckets(aggregateClauses, buckets, response.getHits().getTotalHits());
            extractedMetrics = sortBuckets(query.getOrderByClauses(), extractedMetrics);
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
            // Do not use Collectors.toMap because it does not work with null values.
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.putAll(searchHit.getSourceAsMap());
            map.put("_id", searchHit.getId());
            return map;
        }).collect(Collectors.toList());

    }

    private static List<Map<String, Object>> extractDistinct(Query query, MultiBucketsAggregation aggResult) {
        String field = query.getSelectClause().getFieldClauses().get(0).getField();

        final List<Map<String, Object>> unsortedDistinctValues = new ArrayList<>();
        aggResult.getBuckets().forEach(a -> {
            Map<String, Object> accumulator = new LinkedHashMap<>();
            accumulator.put(field, a.getKey());
            unsortedDistinctValues.add(accumulator);
        });

        List<Map<String, Object>> distinctValues = sortDistinct(query, unsortedDistinctValues);

        int offset = ElasticsearchQueryConverter.getOffset(query);
        int limit = ElasticsearchQueryConverter.getLimit(query, true);

        if (limit == 0) {
            limit = distinctValues.size();
        }

        int endIndex = Math.max(Math.min((limit + offset), distinctValues.size()), offset);

        distinctValues = ((offset >= distinctValues.size()) ? new ArrayList<>() : distinctValues.subList(offset, endIndex));

        return distinctValues;
    }

    private static List<Map<String, Object>> sortDistinct(Query query, List<Map<String, Object>> values) {
        String field = query.getSelectClause().getFieldClauses().get(0).getField();

        List<OrderByFieldClause> orderByFieldClauses = query.getOrderByClauses().stream().filter(orderClause ->
            orderClause instanceof OrderByFieldClause && orderClause.getFieldOrOperation().equals(field)
        ).map(orderClause -> (OrderByFieldClause) orderClause).collect(Collectors.toList());

        if (orderByFieldClauses.size() > 0) {
            OrderByFieldClause orderByFieldClause = orderByFieldClauses.get(0);
            values.sort(new Comparator<Map<String, Object>>() {
                @Override
                @SuppressWarnings({"rawtypes", "unchecked"})
                public int compare(Map<String, Object> a, Map<String, Object> b) {
                    if(a instanceof Comparable && b instanceof Comparable) {
                        return orderByFieldClause.getOrder().getDirection() *
                            ((Comparable) a.get(field)).compareTo(((Comparable) b.get(field)));
                    }
                    if(isNumeric(a.get(field).toString()) && isNumeric(b.get(field).toString())) {
                        return orderByFieldClause.getOrder().getDirection() * (Double.valueOf(a.get(field).toString()))
                            .compareTo(Double.valueOf(b.get(field).toString()));
                    }
                    return orderByFieldClause.getOrder().getDirection() *
                        a.get(field).toString().compareTo(b.get(field).toString());
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

        Map<String, Object> metrics = clauses.stream().filter(aggClause -> !aggClause.getOperation().equals("count") &&
            aggClause instanceof AggregateByFieldClause).map(aggClause -> (AggregateByFieldClause) aggClause)
            .collect(Collectors.toMap(aggClause -> aggClause.getLabel(), aggClause -> {
                Stats result = (Stats) results.get(ElasticsearchQueryConverter.STATS_AGG_PREFIX +
                    aggClause.getField());
                Double metric = 0.0;
                switch (aggClause.getOperation()) {
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

        metrics.putAll(clauses.stream().filter(aggClause -> aggClause instanceof AggregateByTotalCountClause)
            .collect(Collectors.toMap(aggClause -> aggClause.getLabel(), aggClause -> totalCount)));

        metrics.putAll(clauses.stream().filter(aggClause -> !(aggClause instanceof AggregateByTotalCountClause) &&
            aggClause.getOperation().equals("count")).collect(Collectors.toMap(aggClause -> aggClause.getLabel(),
            aggClause -> {
                TransformedAggregationData data = groupResults.get(aggClause instanceof AggregateByGroupCountClause ?
                    ((AggregateByGroupCountClause) aggClause).getGroup() :
                    ((AggregateByFieldClause) aggClause).getField());
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
            accumulator.put(currentClause.getField(), new TransformedAggregationData(bucket.getDocCount(),
                bucket.getKey()));
        } else if (currentClause instanceof GroupByOperationClause) {
            // Date groups will return numbers (year=2018, month=12, day=30, etc.)
            String key = bucket.getKeyAsString();

            boolean isDateClause = Arrays.asList(ElasticsearchQueryConverter.DATE_OPERATIONS)
                    .indexOf(((GroupByOperationClause) currentClause).getOperation()) >= 0 && isNumeric(key);

            accumulator.put(((GroupByOperationClause) currentClause).getLabel(), new TransformedAggregationData(
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

    private static List<Map<String, Object>> sortBuckets(List<OrderByClause> orderClauses,
            List<Map<String, Object>> buckets) {
        if (orderClauses != null && orderClauses.size() > 0) {
            buckets.sort((a, b) -> {
                for (OrderByClause orderClause : orderClauses) {
                    Object aField = a.get(orderClause.getFieldOrOperation());
                    Object bField = b.get(orderClause.getFieldOrOperation());
                    int order = 0;

                    if(isFieldDouble(aField.toString()) && isFieldDouble(bField.toString())) {
                        Double aFieldAsDouble = Double.parseDouble(aField.toString());
                        Double bFieldAsDouble = Double.parseDouble(bField.toString());

                        order = orderClause.getOrder().getDirection() * (aFieldAsDouble.compareTo(bFieldAsDouble));
                    } else {
                        order = orderClause.getOrder().getDirection() * (aField.toString().compareTo(bField.toString()));
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
        int offset = ElasticsearchQueryConverter.getOffset(query);
        int limit = ElasticsearchQueryConverter.getLimit(query, true);

        if (limit == 0) {
            limit = buckets.size();
        }

        int endIndex = Math.max(Math.min((limit + offset), buckets.size()), offset);

        List<Map<String, Object>> result = (offset >= buckets.size()) ? new ArrayList<>() : buckets.subList(offset, endIndex);

        return result;
    }
}
