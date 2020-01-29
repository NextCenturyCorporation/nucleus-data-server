package com.ncc.neon.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.exception.UpsertException;
import com.ncc.neon.models.BetterFile;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Map;

@Component
public class BetterFileService {
    private final RestHighLevelClient elasticSearchClient;
    private final String fileIndex = "file";
    private final String fileDataType = "file";

    @Autowired
    BetterFileService(@Value("${db_host}") String dbHost) {
        this.elasticSearchClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost(dbHost, 9200, "http")
        ));
    }

    public Mono<BetterFile> getById(String id) {
        // Get the file doc by id.
        GetRequest gr = new GetRequest(fileIndex, fileDataType, id);

        return Mono.create(sink -> {
            try {
                GetResponse response = elasticSearchClient.get(gr, RequestOptions.DEFAULT);

                if (response.getSource() == null) {
                    sink.error(new Exception("File " + id + " not found."));
                } else {
                    BetterFile res = new ObjectMapper().readValue(response.getSourceAsString(), BetterFile.class);
                    sink.success(res);
                }
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Flux<Tuple2<String, RestStatus>> initMany(String[] filesToAdd) {
        return Flux.fromArray(filesToAdd)
                .flatMap(fileToAdd -> upsert(new BetterFile(fileToAdd, 0)));
    }

    public Flux<Tuple2<String, RestStatus>> upsertMany(BetterFile[] filesToAdd) {
        return Flux.fromArray(filesToAdd).flatMap(this::upsert);
    }

    public Mono<Tuple2<String, RestStatus>> upsert(BetterFile fileToAdd) {
        // Serialize file to json map.
        @SuppressWarnings("unchecked")
		Map<String, Object> bfMapper = new ObjectMapper().convertValue(fileToAdd, Map.class);

        // Build the elasticsearch request.
        IndexRequest indexRequest = new IndexRequest(fileIndex, fileDataType).source(bfMapper).id(fileToAdd.getFilename());

        // Wrap the async part in a mono.
        return Mono.create(sink -> {
            try {
                IndexResponse indexResponse = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                if (indexResponse.status() != RestStatus.CREATED && indexResponse.status() != RestStatus.OK) {
                    sink.error(new UpsertException(fileToAdd.getFilename()));
                } else {
                    sink.success(Tuples.of(fileToAdd.getFilename(), indexResponse.status()));
                }
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<RestStatus> deleteById(String id) {
        DeleteRequest dr = new DeleteRequest(fileIndex, fileDataType, id);

        return Mono.create(sink -> {
            try {
                DeleteResponse response = elasticSearchClient.delete(dr, RequestOptions.DEFAULT);
                sink.success(response.status());
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }

    public Mono<RestStatus> refreshFilesIndex() {
        return Mono.create(sink -> {
            try {
                RefreshResponse refreshResponse = elasticSearchClient.indices().refresh(new RefreshRequest(fileIndex), RequestOptions.DEFAULT);
                sink.success(refreshResponse.getStatus());
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }
}
