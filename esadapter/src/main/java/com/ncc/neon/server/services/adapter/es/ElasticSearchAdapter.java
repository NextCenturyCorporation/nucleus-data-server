package com.ncc.neon.server.services.adapter.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.GroupByClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.GroupByFunctionClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.result.FieldTypePair;
import com.ncc.neon.server.models.query.result.TabularQueryResult;
import com.ncc.neon.server.services.adapters.QueryAdapter;

import org.apache.http.HttpHost;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/**
 * ElasticSearchAdapter
 */
public class ElasticSearchAdapter implements QueryAdapter {

    RestHighLevelClient client;

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

        void setAggregatedValues(Map<String, Aggregation> newValues) {
            aggregatedValues = newValues;
        }

        long getDocCount() {
            return docCount;
        }

        void setDocCount(long newCount) {
            docCount = newCount;
        }

        private Map<String, Object> groupByKeys;
        private Map<String, Aggregation> aggregatedValues = new HashMap<>();
        private long docCount;
    }

    public ElasticSearchAdapter(String host) {
        this.client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, 9200)));
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query, QueryOptions options) {
        List<AggregateClause> aggregates = query.getAggregates();
        List<GroupByClause> groupByClauses = query.getGroupByClauses();

        checkDatabaseAndTableExists(query.getFilter().getDatabaseName(), query.getFilter().getTableName());
        long d1 = new Date().getTime();

        ElasticSearchRestConversionStrategy conversionStrategy = new ElasticSearchRestConversionStrategy();
        
        SearchRequest request = conversionStrategy.convertQuery(query, options);

        SearchResponse response = this.client.search(request, RequestOptions.DEFAULT);

        Aggregations aggResults = response.getAggregations();

        TabularQueryResult returnVal;

        if (aggregates != null && groupByClauses == null) {
            // LOGGER.debug("aggs and no group by ");
            Map<String, Object> metrics = extractMetrics(aggregates, aggResults != null ? aggResults.asMap() : null,
                    response.getHits().getTotalHits());
            returnVal = new TabularQueryResult(List.of(metrics));
        } else if (groupByClauses != null) {
            // LOGGER.debug("group by ");
            List<AggregationBucket> buckets = extractBuckets(groupByClauses,
                    (MultiBucketsAggregation) aggResults.asList().get(0));
            buckets = combineDuplicateBuckets(buckets);
            List<Map<String, Object>> extractedMetrics = extractMetricsFromBuckets(aggregates, buckets);
            extractedMetrics = sortBuckets(query.getSortClauses(), extractedMetrics);
            extractedMetrics = limitBuckets(extractedMetrics, query);
            returnVal = new TabularQueryResult(extractedMetrics);
        } else if (query.isDistinct()) {
            // LOGGER.debug("distinct");

            returnVal = new TabularQueryResult(
                    extractDistinct(query, (MultiBucketsAggregation) aggResults.asList().get(0)));
        } else if (response.getScrollId() != null) {
            returnVal = collectScrolledResults(query, response);
        } else {
            // LOGGER.debug("none of the above");
            returnVal = new TabularQueryResult(extractHitsFromResults(response));
        }

        long diffTime = new Date().getTime() - d1;
        // LOGGER.debug(" Query took: " + diffTime + " ms ");

        return Mono.just(returnVal);
    }

    /**
     * Use scroll interface to get many results. See:
     *
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/5.6/java-rest-high-search-scroll.html
     */
    private TabularQueryResult collectScrolledResults(Query query, SearchResponse firstResponse) {
        List<Map<String, Object>> accumulatedHits = new ArrayList<>();
        SearchResponse response = firstResponse;
        accumulatedHits.addAll(extractHitsFromResults(response));
        String scrollId = response.getScrollId();

        // Keep scrolling until we either get all of the results or we reach the
        // requested limit
        while (response.getHits().getHits().length > 0 && accumulatedHits.size() < response.getHits().getTotalHits()
                && accumulatedHits.size() < query.getLimitClause().getLimit()) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll();
            try {
                response = this.client.scroll(scrollRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            scrollId = response.getScrollId();
            accumulatedHits.addAll(extractHitsFromResults(response));
        }
        // Limit the list to only the desired limit. This is because we added all the
        // results above so
        // the size of the accumulated hits could be more than the limit desired.
        accumulatedHits = accumulatedHits.subList(0,
                Math.min(accumulatedHits.size(), query.getLimitClause().getLimit()));
        return new TabularQueryResult(accumulatedHits);
    }

    private List<Map<String, Object>> extractHitsFromResults(SearchResponse response) {
        Stream<SearchHit> searchHits = Arrays.stream(response.getHits().getHits());
        return searchHits.map(searchHit -> {
            Map<String, Object> map = searchHit.getSourceAsMap();
            map.put("_id", searchHit.getId());
            return map;
        }).collect(Collectors.toList());

    }

    List<Map<String, Object>> extractDistinct(Query query, MultiBucketsAggregation aggResult) {
        String field = query.getFields().get(0);

        final List<Map<String, Object>> unsortedDistinctValues = new ArrayList<>();
        aggResult.getBuckets().forEach(a -> {
            Map<String, Object> accumulator = new HashMap<>();
            accumulator.put(field, a.getKey());
            unsortedDistinctValues.add(accumulator);
        });

        List<Map<String, Object>> distinctValues = sortDistinct(query, unsortedDistinctValues);

        int offset = ElasticSearchRestConversionStrategy.getOffset(query);
        int limit = ElasticSearchRestConversionStrategy.getLimit(query, true);

        if (limit == 0) {
            limit = distinctValues.size();
        }

        int endIndex = ((limit - 1) + offset) < (distinctValues.size() - 1) ? ((limit - 1) + offset)
                : (distinctValues.size() - 1);
        endIndex = (endIndex > offset ? endIndex : offset);
        distinctValues = ((offset >= distinctValues.size()) ? new ArrayList<>()
                : distinctValues.subList(offset, endIndex));

        return distinctValues;
    }

    private List<Map<String, Object>> sortDistinct(Query query, List<Map<String, Object>> values) {

        if (!query.getSortClauses().isEmpty()) {
            String firstField = query.getFields().get(0);

            values.sort(new Comparator<Map<String, Object>>() {

                @Override
                public int compare(Map<String, Object> a, Map<String, Object> b) {
                    // TODO: does this mean the map is <String,String>
                    return a.get(firstField).toString().compareTo(b.get(firstField).toString());
                }
            });

        }
        return values;
    }

    private static Map<String, Object> extractMetrics(List<AggregateClause> clauses, Map<String, Aggregation> results,
            long totalCount) {

        Map<Boolean, List<AggregateClause>> groups = clauses.stream()
                .collect(Collectors.partitioningBy(it -> ElasticSearchRestConversionStrategy.isCountAllAggregation(it)
                        || ElasticSearchRestConversionStrategy.isCountFieldAggregation(it)));

        List<AggregateClause> countAllClause = groups.get(true);
        List<AggregateClause> metricClauses = groups.get(false);

        Map<String, Object> metrics = metricClauses.stream()
                .collect(Collectors.toMap(clause -> clause.getName(), clause -> {
                    Stats result = (Stats) results
                            .get(ElasticSearchRestConversionStrategy.STATS_AGG_PREFIX + clause.getField());
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

        if (!countAllClause.isEmpty()) {
            metrics.put(countAllClause.get(0).getName(), totalCount);
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
        return extractBuckets(groupByClauses, value, new HashMap<>(), new ArrayList<>());

    }

    private static List<AggregationBucket> extractBuckets(List<GroupByClause> groupByClauses,
            MultiBucketsAggregation value, Map<String, Object> accumulator, List<AggregationBucket> results) {
        value.getBuckets().forEach(bucket -> {
            Map<String, Object> newAccumulator = new HashMap<>();
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
        Map<Map<String, Object>, AggregationBucket> mappedBuckets = new HashMap<>();
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
                new HashMap<String, Object>());
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
                    int order = sortClause.getSortDirection() * (aField.toString().compareTo(bField.toString()));
                    if (order != 0) {
                        return order;
                    }
                }
                return 0;
            });
        }

        return buckets;
    }

    private List<Map<String, Object>> limitBuckets(List<Map<String, Object>> buckets, Query query) {
        int offset = ElasticSearchRestConversionStrategy.getOffset(query);
        int limit = ElasticSearchRestConversionStrategy.getLimit(query, true);

        if (limit == 0) {
            limit = buckets.size();
        }

        int endIndex = ((limit - 1) + offset) < (buckets.size() - 1) ? ((limit - 1) + offset) : (buckets.size() - 1);
        endIndex = endIndex > offset ? endIndex : offset;

        List<Map<String, Object>> result = (offset >= buckets.size()) ? new ArrayList<>()
                : buckets.subList(offset, endIndex);

        return result;
    }

    /**
     * Note: This method is not an appropriate check for queries against index
     * mappings as they allow both the databaseName and tableName to be wildcarded.
     * This method allows only the databaseName to be wildcarded to match the
     * behavior of index searches.
     */
    protected void checkDatabaseAndTableExists(String databaseName, String tableName) {

        if (showTables(tableName).collectList().block().indexOf(tableName) >= 0) {
            throw new ResourceNotFoundException("Table ${tableName} does not exist");
        }
    }

    // TODO: generalize getting flux further?
    @Override
    public Flux<String> showDatabases() {
        GetIndexRequest request = new GetIndexRequest().indices("*");
        return Flux.create(sink -> {
            client.indices().getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetIndexResponse>() {
                @Override
                public void onResponse(GetIndexResponse response) {
                    String[] indices = response.getIndices();
                    for (String index : indices) {
                        sink.next(index);
                    }
                    sink.complete();
                }

                @Override
                public void onFailure(Exception e) {
                    sink.error(e);
                }
            });
        });

    }

    @Override
    public Flux<String> showTables(String dbName) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(dbName);

        return getMappingRequestToFlux(request, (sink, response) -> {
            response.getMappings().get(dbName).keysIt().forEachRemaining(type -> sink.next(type));
        });

    }

    @Override
    public Flux<String> getFieldNames(String dbName, String tableName) {
        BiConsumer<FluxSink<String>, Map<String, Map>> mappingConsumer = (sink, mappingProperties) -> {
            getMappingProperties(mappingProperties, null).forEach(pair -> sink.next(pair.getField()));
        };
        return getMappings(dbName, tableName, mappingConsumer);

    }

    @Override
    public Flux<FieldTypePair> getFieldTypes(String dbName, String tableName) {
        BiConsumer<FluxSink<FieldTypePair>, Map<String, Map>> mappingConsumer = (sink, mappingProperties) -> {
            getMappingProperties(mappingProperties, null).forEach(pair -> sink.next(pair));
        };
        return getMappings(dbName, tableName, mappingConsumer);
    }

    /*
     * Helper function for getfieldname and getfieldtypes as they are the same
     */
    private <T> Flux<T> getMappings(String dbName, String tableName,
            BiConsumer<FluxSink<T>, Map<String, Map>> mappingConsumer) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(dbName);
        request.types(tableName);

        return getMappingRequestToFlux(request, (sink, response) -> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Map<String, Map> mappingProperties = (Map<String, Map>) response.mappings().get(dbName).get(tableName)
                    .sourceAsMap().get("properties");

            mappingConsumer.accept(sink, mappingProperties);
        });
    }

    /* Recursive function to get all the properties */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<FieldTypePair> getMappingProperties(Map<String, Map> mappingProperties, String parentFieldName) {
        List<FieldTypePair> fieldTypePairs = new ArrayList<>();
        mappingProperties.forEach((fieldName, value) -> {
            String type = null;
            if (value.get("type") != null) {
                type = value.get("type").toString();
                if (parentFieldName != null) {
                    fieldName = parentFieldName + "." + fieldName;
                }
                fieldTypePairs.add(new FieldTypePair(fieldName, type));
            } else if (value.get("properties") != null) {
                Map<String, Map> subMapping = (Map<String, Map>) value.get("properties");
                fieldTypePairs.addAll(getMappingProperties(subMapping, fieldName));
            }
        });
        return fieldTypePairs;
    }

    /* Every method need to convert into a flux */
    private <T> Flux<T> getMappingRequestToFlux(GetMappingsRequest request,
            BiConsumer<FluxSink<T>, GetMappingsResponse> responseHandler) {
        return Flux.create(sink -> {
            client.indices().getMappingAsync(request, RequestOptions.DEFAULT,
                    new ActionListener<GetMappingsResponse>() {

                        @Override
                        public void onResponse(GetMappingsResponse response) {
                            responseHandler.accept(sink, response);
                            sink.complete();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            sink.error(e);
                        }
                    });
        });
    }

}