package com.ncc.neon.adapters.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.TableWithFields;
import com.ncc.neon.models.results.TabularQueryResult;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.common.xcontent.XContentType;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ElasticsearchAdapter implements QueryAdapter {
    RestHighLevelClient client;

    public ElasticsearchAdapter(String host, int port, String username, String password) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port));
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

    private void logQuery(Query query, SearchRequest request) {
        log.debug("Neon Query:  " + query.toString());
        log.debug("ES Query:  " + request.toString());
    }

    private void logResults(SearchResponse response, TabularQueryResult results) {
        log.debug("ES Results:  " + response.toString());
        log.debug("Neon Results:  " + results.toString());
    }

    @Override
    public Mono<TabularQueryResult> execute(Query query) {
        checkDatabaseAndTableExists(query);

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

    /**
     * Note: This method is not an appropriate check for queries against index
     * mappings as they allow both the databaseName and tableName to be wildcarded.
     * This method allows only the databaseName to be wildcarded to match the
     * behavior of index searches.
     */
    private void checkDatabaseAndTableExists(Query query) {
        if (query == null || query.getFilter() == null) {
            throw new ResourceNotFoundException("Query does not exist");
        }

        // TODO: Fix (THOR-1077) - commenting out for now
        //String tableName = query.getFilter().getTableName();
        //if(showTables(tableName).collectList().block().indexOf(tableName) >= 0) {
        //    throw new ResourceNotFoundException("Table ${tableName} does not exist");
        //}
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

        // ES6 indexes have databaseName->tableName->"properties" but ES7 indexes have databaseName->"properties"
        // This won't work on an ES6 index with a tableName="properties"
        if (tableName.equals("properties")) {
            return (Map<String, Map>) mappingTable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                    entry -> (Map) entry.getValue()));
        }
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
                fieldTypePairs.add(new FieldTypePair(fieldName, type));
            } else if (value.get("properties") != null) {
                Map<String, Map> nestedFields = (Map<String, Map>) value.get("properties");
                fieldTypePairs.addAll(getFieldsFromMapping(nestedFields, fieldName));
            }
        });
        return fieldTypePairs;
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

    @Override
    public Mono<Boolean> addData(String databaseName, String tableName, TabularQueryResult newData) {
//         BulkRequestBuilder bulkRequest = this.client.prepareBulk();
//         bulkRequest.add(
//                 client.prepareIndex(databaseName, tableName).setSource(newData, TabularQueryResult));
//        BulkRequest request = new BulkRequest();
//        BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();
//
//        BufferedReader br = new BufferedReader(newData);
//
//        String line;
//
//        while ((line = br.readLine()) != null) {
//            bulkProcessor.add(new IndexRequest(databaseName, tableName).source(line, XContentType.JSON));
//        }

        long t=System.currentTimeMillis();
        try {
            Response response = client.getLowLevelClient().performRequest("HEAD", "/" + databaseName);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 404) {
//                CreateIndexRequest cireq = new CreateIndexRequest(databaseName);
//                CreateIndexResponse ciresp = client.indices().create(cireq);
                System.out.println("Index does not exist");
            } else {
                System.out.println("Index exists");
            }


            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                int count = 0;

                @Override
                public void beforeBulk(long l, BulkRequest bulkRequest) {
                    count = count + bulkRequest.numberOfActions();
                    System.out.println("Uploaded " + count + " so far");
                }

                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                    if (bulkResponse.hasFailures()) {
                        for (BulkItemResponse bulkItemResponse : bulkResponse) {
                            if (bulkItemResponse.isFailed()) {
                                System.out.println(bulkItemResponse.getOpType());
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                System.out.println("Error " + failure.toString());
                            }
                        }
                    }
                }

                @Override
                public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                    System.out.println("Big errors " + throwable.toString());
                }
            };

            BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();

//            BufferedReader br = new BufferedReader(new FileReader("enron.json"));
//
//            String line;
//
//            while ((line = br.readLine()) != null) {
            bulkProcessor.add(new IndexRequest(databaseName, databaseName).source(newData, XContentType.JSON));
//            }

            System.out.println("Waiting to finish");

            boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
            if (!terminated) {
                System.out.println("Some requests have not been processed");
            }

            client.close();
            long tn = System.currentTimeMillis();
            System.out.println("Took " + (tn - t) / 1000 + " seconds");

            return Mono.just(terminated);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return Mono.just(false);
    }

}
