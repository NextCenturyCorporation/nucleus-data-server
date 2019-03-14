package com.ncc.neon.server.services.adapter.es;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.GroupByClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.GroupByFunctionClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.result.TabularQueryResult;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

public class ElasticsearchTransformer {

    /*
     * A small class to hold the important information about an aggregation bucket
     * between when the buckets are taken out of ElasticSearch's hierarchical
     * arrangement and when everything can be extracted into the format that the
     * Neon API uses.
     */

    private static class AggregationBucket {
        Map<String, Object> getGroupByKeys() {
            return groupByKeys;
        }

        void setGroupByKeys(Map<String, Object> newGroupByKeys) {
            groupByKeys = newGroupByKeys;
        }

        Map<String, Aggregation> getAggregatedValues() {
            return aggregatedValues;
        }

        long getDocCount() {
            return docCount;
        }

        void setDocCount(long newCount) {
            docCount = newCount;
        }

        private Map<String, Object> groupByKeys;
        private Map<String, Aggregation> aggregatedValues = new LinkedHashMap<>();
        private long docCount;
    }

    public ElasticsearchTransformer() {
    }

    public static SearchRequest transformQuery(Query query, QueryOptions options) {
        ElasticSearchRestConversionStrategy conversionStrategy = new ElasticSearchRestConversionStrategy();
        return conversionStrategy.convertQuery(query, options);
    }

    public static TabularQueryResult transformResults(Query query, QueryOptions options, SearchResponse response) {
        List<AggregateClause> aggregateClauses = query.getAggregates();
        List<GroupByClause> groupByClauses = query.getGroupByClauses();

        Aggregations aggregationResults = response.getAggregations();

        TabularQueryResult results;

        if (aggregateClauses.size() > 0 && groupByClauses.size() == 0) {
            Map<String, Object> metrics = extractMetrics(aggregateClauses, aggregationResults != null ? aggregationResults.asMap() : null, response.getHits().getTotalHits());
            results = new TabularQueryResult(List.of(metrics));
        } else if (aggregateClauses.size() > 0 && groupByClauses.size() > 0) {
            List<AggregationBucket> buckets = extractBuckets(groupByClauses, (MultiBucketsAggregation) aggregationResults.asList().get(0));
            buckets = combineDuplicateBuckets(buckets);
            List<Map<String, Object>> extractedMetrics = extractMetricsFromBuckets(aggregateClauses, buckets);
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

        int offset = ElasticSearchRestConversionStrategy.getOffset(query);
        int limit = ElasticSearchRestConversionStrategy.getLimit(query, true);

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

        Map<Boolean, List<AggregateClause>> groups = clauses.stream().collect(Collectors.partitioningBy(it ->
            ElasticSearchRestConversionStrategy.isCountAllAggregation(it) ||
            ElasticSearchRestConversionStrategy.isCountFieldAggregation(it)));

        List<AggregateClause> countClauses = groups.get(true);
        List<AggregateClause> metricsClauses = groups.get(false);

        Map<String, Object> metrics = metricsClauses.stream().collect(Collectors.toMap(clause -> clause.getName(), clause -> {
            Stats result = (Stats) results.get(ElasticSearchRestConversionStrategy.STATS_AGG_PREFIX + clause.getField());
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

        if (!countClauses.isEmpty()) {
            metrics.put(countClauses.get(0).getName(), totalCount);
        }

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
    private static List<AggregationBucket> extractBuckets(List<GroupByClause> groupByClauses,
            MultiBucketsAggregation value) {
        return extractBuckets(groupByClauses, value, new LinkedHashMap<>(), new ArrayList<>());

    }

    private static List<AggregationBucket> extractBuckets(List<GroupByClause> groupByClauses,
            MultiBucketsAggregation value, Map<String, Object> accumulator, List<AggregationBucket> results) {
        value.getBuckets().forEach(bucket -> {
            Map<String, Object> newAccumulator = new LinkedHashMap<>();
            newAccumulator.putAll(accumulator);
            extractBucket(groupByClauses, bucket, newAccumulator, results);

        });

        return results;
    }

    public static boolean isNumeric(String inputData) {
        return inputData.matches("[-+]?\\d+(\\.\\d+)?");
    }

    private static void extractBucket(List<GroupByClause> groupByClauses, Bucket value, Map<String, Object> accumulator,
            List<AggregationBucket> results) {
        GroupByClause currentClause = groupByClauses.get(0);

        if (currentClause instanceof GroupByFieldClause) {
            accumulator.put(((GroupByFieldClause) currentClause).getField(), value.getKey());
        } else if (currentClause instanceof GroupByFunctionClause) {
            // If the group by field is a function of a date (e.g., group by month), then
            // the
            // key field will still be just a date, but getKeyAsString() will return the
            // value
            // returned by the function (e.g., the month).
            String key = value.getKeyAsString();

            boolean isDateClause = Arrays.asList(ElasticSearchRestConversionStrategy.DATE_OPERATIONS)
                    .indexOf(((GroupByFunctionClause) currentClause).getOperation()) >= 0 && isNumeric(key);

            // TODO Why does the date need to be a float?
            accumulator.put(((GroupByFunctionClause) currentClause).getName(),
                    isDateClause ? Float.parseFloat(key) : key);
        } else {
            throw new RuntimeException("Bad implementation - ${currentClause.getClass()} is not a valid groupByClause");
            // throw new NeonConnectionException("Bad implementation -
            // ${currentClause.getClass()} is not a valid groupByClause");
        }

        List<GroupByClause> tail = groupByClauses.subList(1, groupByClauses.size());
        if (tail != null && tail.size() > 0) {
            extractBuckets(tail, (MultiBucketsAggregation) value.getAggregations().asList().get(0), accumulator,
                    results);
        } else {
            AggregationBucket bucket = new AggregationBucket();
            bucket.setGroupByKeys(accumulator);
            bucket.setDocCount(value.getDocCount());

            Aggregations terminalAggs = value.getAggregations();
            if (terminalAggs != null) {
                bucket.getAggregatedValues().putAll(terminalAggs.asMap());
            }
            results.add(bucket);
        }
    }

    private static List<AggregationBucket> combineDuplicateBuckets(List<AggregationBucket> buckets) {
        Map<Map<String, Object>, AggregationBucket> mappedBuckets = new LinkedHashMap<>();
        // Iterate over all of the buckets, looking for any that have the same
        // groupByKeys

        buckets.forEach(bucket -> {
            // Only process a bucket if there are documents in it, since we're using a
            // histogram to
            // replicate group-by functionality.
            if (bucket.getDocCount() > 0) {
                AggregationBucket existingBucket = mappedBuckets.get(bucket.getGroupByKeys());
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
                            throw new RuntimeException("Look at the code something is crazy");
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
            List<AggregationBucket> buckets) {

        return buckets.stream().map(bucket -> {
            Map<String, Object> result = bucket.getGroupByKeys();
            result.putAll(extractMetrics(clauses, bucket.getAggregatedValues(), bucket.getDocCount()));
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
        int offset = ElasticSearchRestConversionStrategy.getOffset(query);
        int limit = ElasticSearchRestConversionStrategy.getLimit(query, true);

        if (limit == 0) {
            limit = buckets.size();
        }

        int endIndex = Math.max(Math.min((limit + offset), buckets.size()), offset);

        List<Map<String, Object>> result = (offset >= buckets.size()) ? new ArrayList<>() : buckets.subList(offset, endIndex);

        return result;
    }
}
