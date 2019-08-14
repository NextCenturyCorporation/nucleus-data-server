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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchAdapter implements QueryAdapter {
    RestHighLevelClient client;

    public ElasticsearchAdapter(String target) {
        String[] targetData = target.split("@");
        String[] hostData = targetData[(targetData.length > 1 ? 1 : 0)].split(":");
        String host = hostData[0];
        int port = hostData.length > 1 ? Integer.parseInt(hostData[1]) : 9200;
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port));
        if(targetData.length > 1) {
            String[] userData = targetData[0].split(":");
            if(userData.length > 1) {
                String username = userData[0];
                String password = userData[1];
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                builder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
            }
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

        if(response != null) {
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
        if(query == null || query.getFilter() == null) {
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Flux<TableWithFields> getTableAndFieldNames(String databaseName) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(databaseName);

        return getMappingRequestToFlux(request, (sink, response) -> {
            response.getMappings().get(databaseName).keysIt().forEachRemaining(tableName -> {
                Map<String, Map> mappingProperties = getPropertiesFromMapping(response, databaseName, tableName);
                List<String> fieldNames = getFieldNamesFromMapping(mappingProperties, null);
                if(!fieldNames.contains("_id")) {
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
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

}
