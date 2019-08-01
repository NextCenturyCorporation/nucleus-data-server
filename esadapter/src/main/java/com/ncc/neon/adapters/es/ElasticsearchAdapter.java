package com.ncc.neon.adapters.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.ncc.neon.adapters.QueryAdapter;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.results.FieldTypePair;
import com.ncc.neon.models.results.TableWithFields;
import com.ncc.neon.models.results.TabularQueryResult;

import org.apache.http.HttpHost;
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
import org.elasticsearch.client.RestHighLevelClient;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchAdapter implements QueryAdapter {
    RestHighLevelClient client;

    public ElasticsearchAdapter(String host) {
        this.client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, 9200)));
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Flux<TableWithFields> getTableAndFieldNames(String dbName) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(dbName);

        return getMappingRequestToFlux(request, (sink, response) -> {
            response.getMappings().get(dbName).keysIt().forEachRemaining(tableName -> {

                Map<String, Map> mappingProperties = (Map<String, Map>) response.mappings().get(dbName).get(tableName).sourceAsMap().get("properties");

                List<String> fieldNames = getMappingPropertiesFieldNamesOnly(mappingProperties, null);
                if(!fieldNames.contains("_id")) {
                    fieldNames.add("_id");
                }
                sink.next(new TableWithFields(tableName, fieldNames));
            });
        });
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T> Flux<T> getMappings(String dbName, String tableName,
            BiConsumer<FluxSink<T>, Map<String, Map>> mappingConsumer) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(dbName);
        request.types(tableName);

        return getMappingRequestToFlux(request, (sink, response) -> {
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

    /* Recursive function to get all the field names */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<String> getMappingPropertiesFieldNamesOnly(Map<String, Map> mappingProperties, String parentFieldName) {
        List<String> fields = new ArrayList<>();
        mappingProperties.forEach((fieldName, value) -> {
            /* If we want to include parents of nested fields in list:
            if (parentFieldName != null) {
                fieldName = parentFieldName + "." + fieldName;
            }
            fields.add(fieldName);
            if (value.get("properties") != null) {
                Map<String, Map> subMapping = (Map<String, Map>) value.get("properties");
                fields.addAll(getMappingPropertiesFieldNamesOnly(subMapping, fieldName));
            }*/
            if (value.get("type") != null) {
                if (parentFieldName != null) {
                    fieldName = parentFieldName + "." + fieldName;
                }
                fields.add(fieldName);
            } else if (value.get("properties") != null) {
                Map<String, Map> subMapping = (Map<String, Map>) value.get("properties");
                fields.addAll(getMappingPropertiesFieldNamesOnly(subMapping, fieldName));
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
