package com.ncc.neon.adapters.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.FieldType;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.ImportResult;
import com.ncc.neon.models.results.TableWithFields;
import com.ncc.neon.models.results.TabularQueryResult;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.Failure;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

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

        SearchRequest request = ElasticsearchQueryConverter.convertQuery(query);
        SearchResponse response = null;
        TabularQueryResult results = null;

        try {
            response = this.client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (response != null) {
            results = ElasticsearchResultsConverter.convertResults(query, response);
        }

        return Mono.just(results);
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

    @SuppressWarnings("deprecation")
    @Override
    public Mono<ImportResult> addData(String databaseName, String tableName, List<String> sourceData) {

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(TimeValue.timeValueMinutes(1));
        sourceData.forEach((String record) -> {
            bulkRequest.add(new IndexRequest(databaseName, tableName).source(record, XContentType.JSON));
        });

        return Mono.create(sink -> {
        client.bulkAsync(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    List<String> recordErrors = new ArrayList<>();

                    if (bulkResponse.hasFailures())
                    {
                        recordErrors = Arrays.stream(bulkResponse.getItems())
                        .filter(item -> item.isFailed()).map((BulkItemResponse item) -> {
                            return String.format("%d,%s", item.getFailure().getSeqNo(), item.getFailure().getMessage());
                        }).collect(Collectors.toList());
                    }

                    sink.success(new ImportResult(recordErrors));

                }

                @Override
                public void onFailure(Exception e) {
                    sink.error(e);
                }
            });
        });
    }

}
