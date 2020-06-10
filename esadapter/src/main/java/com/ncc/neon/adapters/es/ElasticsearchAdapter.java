package com.ncc.neon.adapters.es;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.ImportQuery;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public class ElasticsearchAdapter extends QueryAdapter {
    static final int DEFAULT_PORT = 9200;

    private RestHighLevelClient client;

    public ElasticsearchAdapter(String host, String usernameFromConfig, String passwordFromConfig) {
        super("Elasticsearch", host, usernameFromConfig, passwordFromConfig);

        // Expect host to be "host", "username@host", or "username:password@host" (ending with optional ":port")
        String[] hostAndAuthData = host.split("@");
        String[] hostAndPortData = hostAndAuthData[(hostAndAuthData.length > 1 ? 1 : 0)].split(":");
        String hostOnly = hostAndPortData[0];
        int port = hostAndPortData.length > 1 ? Integer.parseInt(hostAndPortData[1]) : DEFAULT_PORT;

        String[] userAndPassData = hostAndAuthData.length > 1 ? hostAndAuthData[0].split(":") : new String[] {};
        String username = usernameFromConfig != null ? usernameFromConfig : (userAndPassData.length > 0 ?
            userAndPassData[0] : null);
        String password = passwordFromConfig != null ? passwordFromConfig : (userAndPassData.length > 1 ?
            userAndPassData[1] : null);

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostOnly, port));
        if (username != null && password != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                }
            });
        }
        this.client = new RestHighLevelClient(builder);
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query) {
        verifyQueryTablesExist(query);
        log.debug("Neon query: " + query.toString());

        SearchRequest request = ElasticsearchQueryConverter.convertQuery(query);
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        request.scroll(scroll);
        SearchResponse response = null;
        TabularQueryResult results = null;
        List<Map<String, Object>> collectedResults = null;
        boolean bigLimit = (query.getLimitClause() != null && query.getLimitClause().getLimit() > ElasticsearchQueryConverter.MAX_QUERY_LIMIT);

        try {
            if (bigLimit && query.getAggregateClauses() != null && !query.getAggregateClauses().isEmpty()) {
                int numPartitions = query.getLimitClause().getLimit() / ElasticsearchQueryConverter.PARTITIONED_AGGREGATION_LIMIT;

                TermsAggregationBuilder termsAB = null;
                Collection<AggregationBuilder> aggregationBuilders = request.source().aggregations()
                        .getAggregatorFactories();
                for (AggregationBuilder aggregation : aggregationBuilders) {
                    if (aggregation instanceof TermsAggregationBuilder) {
                        termsAB = (TermsAggregationBuilder) aggregation;
                    }
                }
                if (termsAB != null) {
                    collectedResults = new ArrayList<>();
                    for (int i = 0; i < numPartitions; i++) {
                        termsAB.includeExclude(new IncludeExclude(i, numPartitions));
                        log.debug("ES Scroll Request: " + request.toString());
                        SearchResponse partitionResponse = this.client.search(request, RequestOptions.DEFAULT);
                        collectedResults.addAll(ElasticsearchResultsConverter.convertResults(query, partitionResponse));
                    }
                    collectedResults = ElasticsearchResultsConverter.sortBuckets(query.getOrderByClauses(), collectedResults);
                }
            } else {
                // over limit regular query requires terminateAfter
                if (bigLimit) {
                    request.source().terminateAfter(query.getLimitClause().getLimit());
                }
                log.debug("ES Search Request: " + request.toString());
                response = this.client.search(request, RequestOptions.DEFAULT);
            }

            if (bigLimit && response != null) {
                collectedResults = ElasticsearchResultsConverter.getScrolledResults(scroll, response, this.client);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (collectedResults != null) {
            results = new TabularQueryResult(collectedResults);
        } else if (response != null) {
            results = new TabularQueryResult(ElasticsearchResultsConverter.convertResults(query, response));
        }

        log.debug("Returning " + results.getData().size() + " results!");
        return Mono.just(results);
    }

    // TODO: generalize getting flux further?
    @SuppressWarnings("deprecation")
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
    public Flux<String> showTables(String databaseName) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(databaseName);

        return getMappingRequestToFlux(request, (sink, response) -> {
            response.getMappings().get(databaseName).keysIt().forEachRemaining(type -> sink.next(type));
        });

    }

    @Override
    public Flux<String> getFieldNames(String databaseName, String tableName) {
        BiConsumer<FluxSink<String>, Map<String, Map>> mappingConsumer = (sink, mappingProperties) -> {
            getFieldsFromMapping(mappingProperties, null).forEach(pair -> sink.next(pair.getField()));
        };
        return getMappings(databaseName, tableName, mappingConsumer);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Flux<TableWithFields> getTableAndFieldNames(String databaseName) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(databaseName);

        return getMappingRequestToFlux(request, (sink, response) -> {
            response.getMappings().get(databaseName).keysIt().forEachRemaining(tableName -> {
                Map<String, Map> mappingProperties = getPropertiesFromMapping(response, databaseName, tableName);
                List<String> fieldNames = getFieldNamesFromMapping(mappingProperties, null);
                if (!fieldNames.contains("_id")) {
                    fieldNames.add("_id");
                }
                sink.next(new TableWithFields(tableName, fieldNames));
            });
        });
    }

    private Map<String, Map> getPropertiesFromMapping(GetMappingsResponse response, String databaseName, String tableName) {
        Map<String, Object> mappingTable = response.mappings().get(databaseName).get(tableName).sourceAsMap();
        return (Map<String, Map>) mappingTable.get("properties");
    }

    @Override
    public Flux<FieldTypePair> getFieldTypes(String databaseName, String tableName) {
        BiConsumer<FluxSink<FieldTypePair>, Map<String, Map>> mappingConsumer = (sink, mappingProperties) -> {
            getFieldsFromMapping(mappingProperties, null).forEach(pair -> sink.next(pair));
        };
        return getMappings(databaseName, tableName, mappingConsumer);
    }

    /*
     * Helper function for getfieldname and getfieldtypes as they are the same
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> Flux<T> getMappings(String databaseName, String tableName,
                                    BiConsumer<FluxSink<T>, Map<String, Map>> mappingConsumer) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(databaseName);

        return getMappingRequestToFlux(request, (sink, response) -> {
            Map<String, Map> mappingProperties = getPropertiesFromMapping(response, databaseName, tableName);
            mappingConsumer.accept(sink, mappingProperties);
        });
    }

    /* Recursive function to get all the properties */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<FieldTypePair> getFieldsFromMapping(Map<String, Map> mappingProperties, String parentFieldName) {
        List<FieldTypePair> fieldTypePairs = new ArrayList<>();
        mappingProperties.forEach((fieldName, value) -> {
            String type = null;
            if (value.get("type") != null) {
                type = value.get("type").toString();
                if (parentFieldName != null) {
                    fieldName = parentFieldName + "." + fieldName;
                }
                fieldTypePairs.add(new FieldTypePair(fieldName, retrieveFieldType(type)));
            } else if (value.get("properties") != null) {
                Map<String, Map> nestedFields = (Map<String, Map>) value.get("properties");
                fieldTypePairs.addAll(getFieldsFromMapping(nestedFields, fieldName));
            }
        });
        fieldTypePairs.add(new FieldTypePair("_id", FieldType.ID));
        return fieldTypePairs;
    }

    private FieldType retrieveFieldType(String type) {
        switch (type) {
            case "boolean":
                return FieldType.BOOLEAN;
            case "byte":
            case "integer":
            case "long":
            case "short":
                return FieldType.INTEGER;
            case "date":
                return FieldType.DATETIME;
            case "double":
            case "float":
            case "half_float":
            case "scaled_float":
                return FieldType.DECIMAL;
            case "geo-point":
            case "geo-shape":
                return FieldType.GEO;
            case "keyword":
                return FieldType.KEYWORD;
            case "nested":
            case "object":
                return FieldType.OBJECT;
            case "text":
            default:
                return FieldType.TEXT;
        }
    }

    /* Recursive function to get all the field names */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<String> getFieldNamesFromMapping(Map<String, Map> mappingProperties, String parentFieldName) {
        List<String> fields = new ArrayList<>();
        mappingProperties.forEach((fieldName, value) -> {
            /* If we want to include parents of nested fields in list:
            if (parentFieldName != null) {
                fieldName = parentFieldName + "." + fieldName;
            }
            fields.add(fieldName);
            if (value.get("properties") != null) {
                Map<String, Map> nestedFields = (Map<String, Map>) value.get("properties");
                fields.addAll(getFieldNamesFromMapping(nestedFields, fieldName));
            }*/
            if (value.get("type") != null) {
                if (parentFieldName != null) {
                    fieldName = parentFieldName + "." + fieldName;
                }
                fields.add(fieldName);
            } else if (value.get("properties") != null) {
                Map<String, Map> nestedFields = (Map<String, Map>) value.get("properties");
                fields.addAll(getFieldNamesFromMapping(nestedFields, fieldName));
            }
        });
        return fields;
    }

    /* Every method need to convert into a flux */
    @SuppressWarnings("deprecation")
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

    @Override
    public Mono<ActionResult> importData(ImportQuery importQuery) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMinutes(1));
        importQuery.getSource().forEach((String record) -> {
            bulkRequest.add(new IndexRequest(importQuery.getDatabase(), importQuery.getTable())
                .source(record, XContentType.JSON));
        });

        return Mono.create(sink -> {
            client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    List<String> documentErrors = new ArrayList<>();

                    if (bulkResponse.hasFailures()) {
                        documentErrors = Arrays.stream(bulkResponse.getItems())
                        .filter(item -> item.isFailed()).map((BulkItemResponse item) -> {
                            return String.format("%d,%s", item.getFailure().getSeqNo(), item.getFailure().getMessage());
                        }).collect(Collectors.toList());
                    }

                    String responseText = "Imported " + bulkResponse.getItems().length + " items successfully.";
                    sink.success(new ActionResult(responseText, documentErrors));
                }

                @Override
                public void onFailure(Exception e) {
                    sink.error(e);
                }
            });
        });
    }

    @Override
    public Mono<ActionResult> mutateData(MutateQuery mutateQuery) {
        if (mutateQuery.getWhereClause() == null) {
            UpdateRequest updateRequest = ElasticsearchQueryConverter.convertMutationByIdQuery(mutateQuery);
            return Mono.create(sink -> {
                client.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
                    @Override
                    public void onResponse(UpdateResponse updateResponse) {
                        processResponse(sink, updateResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        sink.error(e);
                    }
                });
            });
        } else {
            UpdateByQueryRequest updateRequest = ElasticsearchQueryConverter.convertMutationByFilterQuery(mutateQuery);

            return Mono.create(sink -> {
                client.updateByQueryAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        String responseText = response.getUpdated() + " documents updated.";
                        List<String> documentErrors = response.getBulkFailures().isEmpty() ? new ArrayList<String>() : response.getBulkFailures().stream().map(failure -> failure.toString()).collect(Collectors.toList());
                        sink.success(new ActionResult(responseText, documentErrors));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        sink.error(e);
                    }
                });
            });
        }
    }

    @Override
    public Mono<ActionResult> insertData(MutateQuery mutate) {
        IndexRequest indexRequest = ElasticsearchQueryConverter.convertMutationInsertQuery(mutate);
        return Mono.create(sink -> {
            client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {

                @Override
                public void onResponse(IndexResponse indexResponse) {
                    processResponse(sink, indexResponse);
                }

                @Override
                public void onFailure(Exception e) {
                    sink.error(e);
                }
            });
        });
    }

    @Override
    public Mono<ActionResult> deleteData(MutateQuery mutateQuery) {
        if (mutateQuery.getWhereClause() == null) {
            DeleteRequest deleteRequest = ElasticsearchQueryConverter.convertMutationDeleteByIdQuery(mutateQuery);

            return Mono.create(sink -> {
                client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
                    @Override
                    public void onResponse(DeleteResponse deleteResponse) {
                        processResponse(sink, deleteResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        sink.error(e);
                    }
                });
            });
        } else {
            DeleteByQueryRequest deleteRequest = ElasticsearchQueryConverter.convertMutationDeleteByFilterQuery(mutateQuery);

            return Mono.create(sink -> {
                client.deleteByQueryAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        String responseText = response.getDeleted() + " documents deleted.";
                        List<String> documentErrors = response.getBulkFailures().isEmpty() ? new ArrayList<String>() : response.getBulkFailures().stream().map(failure -> failure.toString()).collect(Collectors.toList());
                        sink.success(new ActionResult(responseText, documentErrors));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        sink.error(e);
                    }
                });
            });
        }

    }

    private void processResponse(MonoSink<ActionResult> sink, DocWriteResponse response) {
        String statusText = "";
        boolean responseFailed = false;
        switch (response.getResult()) {
            case CREATED:
                statusText = "created successfully";
                break;
            case UPDATED:
                statusText = "updated successfully";
                break;
            case DELETED:
                statusText = "deleted successfully";
                break;
            case NOOP:
                statusText = "no operation needed";
                break;
            case NOT_FOUND:
                statusText = "not found";
                responseFailed = true;
                break;
        }
        String responseText = "Index " + response.getIndex() + " ID " + response.getId() +
                " " + statusText + ".";
        List<String> documentErrors = responseFailed ? new ArrayList<String>() {{
            add(responseText);
        }} : new ArrayList<String>();
        sink.success(new ActionResult(responseText, documentErrors));
    }
}
