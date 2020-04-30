package com.ncc.neon.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.exception.UpsertException;
import com.ncc.neon.models.DataNotification;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.util.Map;

public abstract class ElasticSearchService<T> {
    protected final RestHighLevelClient elasticSearchClient;
    private final String index;
    private final String dataType;
    private final Class<T> type;
    private final DatasetService datasetService;
    private final DataNotification notification;

    ElasticSearchService(String host, String index, String dataType, Class<T> type, DatasetService datasetService) {
        this.elasticSearchClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost(host, 9200, "http")
        ));
        this.index = index;
        this.dataType = dataType;
        this.type = type;
        this.datasetService = datasetService;
        notification = new DataNotification();
        notification.setTableName(index);
    }

    public Flux<T> getAll() {
        SearchRequest sr = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        sr.source(searchSourceBuilder);

        return Flux.create(tFluxSink -> {
            try {
                SearchResponse response = elasticSearchClient.search(sr, RequestOptions.DEFAULT);
                SearchHit[] hits = response.getHits().getHits();

                for (SearchHit hit : hits) {
                    T res = new ObjectMapper().readValue(hit.getSourceAsString(), type);
                    tFluxSink.next(res);
                }

                tFluxSink.complete();
            } catch (Exception e) {
                tFluxSink.error(e);
            }
        });
    }

    public Mono<T> getById(String id) {
        GetRequest gr = new GetRequest(index, dataType, id);

        return Mono.create(sink -> {
            try {
                GetResponse response = elasticSearchClient.get(gr, RequestOptions.DEFAULT);

                if (response.getSource() == null) {
                    sink.error(new Exception(type.getName() + " " + id + " not found."));
                } else {
                    T res = new ObjectMapper().readValue(response.getSourceAsString(), type);
                    sink.success(res);
                }
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<Long> count(Map<String, Object> fields) {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            boolQueryBuilder = boolQueryBuilder.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()));
        }

        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        return Mono.create(sink -> {
            try {
                SearchResponse searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
                sink.success(searchResponse.getHits().getTotalHits());
            } catch (IOException e) {
                sink.error(e);
            }
        });
    }

    public Mono<Tuple2<String, RestStatus>> insert(T itemToAdd) {
        // Serialize item to json map.
        Map<String, Object> itemMap = new ObjectMapper().convertValue(itemToAdd, Map.class);

        IndexRequest indexRequest = new IndexRequest(index, dataType).source(itemMap);
        return completeIndexRequest(indexRequest);
    }

    public Mono<RestStatus> insertAndRefresh(T itemToInsert) {
        return insert(itemToInsert)
                .then(refreshIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(notification));
    }

    public Mono<RestStatus> upsertAndRefresh(T itemToAdd, String id) {
        return upsert(itemToAdd, id)
                .then(refreshIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(notification));
    }

    public Mono<Tuple2<String, RestStatus>> upsert(T itemToAdd, String id) {
        // Serialize item to json map.
        Map<String, Object> itemMap = new ObjectMapper().convertValue(itemToAdd, Map.class);

        IndexRequest indexRequest = new IndexRequest(index, dataType).source(itemMap).id(id);
        return completeIndexRequest(indexRequest);
    }

    public Mono<RestStatus> updateAndRefresh(Map<String, Object> data, String docId) {
        return update(data, docId)
                .then(refreshIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(notification));
    }

    public Mono<RestStatus> update(Map<String, Object> data, String docId) {
        UpdateRequest request = new UpdateRequest(index, dataType, docId).doc(data);
        request.retryOnConflict(3);
        return Mono.create(sink -> {
            UpdateResponse response = null;
            try {
                response = elasticSearchClient.update(request, RequestOptions.DEFAULT);
                sink.success(response.status());
            } catch (IOException e) {
                sink.error(e);
            }
        });
    }

    public Mono<RestStatus> deleteByIdAndRefresh(String id) {
        return deleteById(id)
                .then(refreshIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(notification));
    }

    public Mono<RestStatus> deleteById(String id) {
        DeleteRequest dr = new DeleteRequest(index, dataType, id);

        return Mono.create(sink -> {
            try {
                DeleteResponse response = elasticSearchClient.delete(dr, RequestOptions.DEFAULT);
                sink.success(response.status());
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<RestStatus> refreshIndex() {
        return Mono.create(sink -> {
            try {
                RefreshResponse refreshResponse = elasticSearchClient.indices().refresh(new RefreshRequest(index), RequestOptions.DEFAULT);
                sink.success(refreshResponse.getStatus());
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    private Mono<Tuple2<String, RestStatus>> completeIndexRequest(IndexRequest request) {
        return Mono.create(sink -> {
            try {
                IndexResponse indexResponse = elasticSearchClient.index(request, RequestOptions.DEFAULT);

                if (indexResponse.status() != RestStatus.CREATED && indexResponse.status() != RestStatus.OK) {
                    sink.error(new UpsertException(request.id(), indexResponse.status().toString()));
                } else {
                    sink.success(Tuples.of(indexResponse.getId(), indexResponse.status()));
                }
            } catch (Exception e) {
                sink.error(new UpsertException(request.id(), e.getMessage()));
            }
        });
    }
}
