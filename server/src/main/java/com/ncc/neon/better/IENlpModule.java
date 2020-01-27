package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Component
public class IENlpModule extends NlpModule {
    private HttpEndpoint trainEndpoint;
    private HttpEndpoint trainListEndpoint;
    private HttpEndpoint infEndpoint;
    private HttpEndpoint infListEndpoint;
    private String shareDir;

    @Autowired
    IENlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        super(datasetService, fileShareService, betterFileService);
        shareDir = System.getenv("SHARE_DIR");
    }

    @Override
    public void setEndpoints(HttpEndpoint[] endpoints) {
        for (HttpEndpoint endpoint : endpoints) {
            switch (endpoint.getType()) {
                case TRAIN:
                    trainEndpoint = endpoint;
                    break;
                case TRAIN_LIST:
                    trainListEndpoint = endpoint;
                    break;
                case INF:
                    infEndpoint = endpoint;
                    break;
                case INF_LIST:
                    infListEndpoint = endpoint;
                    break;
            }
        }
    }

    public Flux<RestStatus> performTraining(String trainConfigFile) throws IOException {
        // Parse JSON file to maps.
        String shareDir = System.getenv("SHARE_DIR");
        File trainConfig = new File(Paths.get(shareDir, trainConfigFile).toString());
        Map<String, String> trainConfigMap = new ObjectMapper().readValue(trainConfig, Map.class);
        Map<String, String> listConfigMap = new HashMap<>();
        // Reuse output_file_prefix field for the list call.
        listConfigMap.put("output_file_prefix", trainConfigMap.get("output_file_prefix"));

        return this.performListOperation(listConfigMap, trainListEndpoint)
                .doOnError(onError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage())))
                .flatMapMany(pendingFiles -> this.initPendingFiles(pendingFiles)
                .then(this.performNlpOperation(trainConfigMap, trainEndpoint))
                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles)))
                .flatMap(this::handleNlpOperationSuccess);
    }

    public Flux<RestStatus> performInference(String listConfigFile, String infConfigFile) throws IOException {
        File listConfig = new File(Paths.get(shareDir, listConfigFile).toString());
        File infConfig = new File(Paths.get(shareDir, infConfigFile).toString());
        Map<String, String> listConfigMap = new ObjectMapper().readValue(listConfig, Map.class);
        Map<String, String> infConfigMap = new ObjectMapper().readValue(infConfig, Map.class);

        return this.performListOperation(listConfigMap, infListEndpoint)
                .doOnError(onError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage())))
                .flatMapMany(pendingFiles -> this.initPendingFiles(pendingFiles)
                .then(this.performNlpOperation(infConfigMap, infEndpoint))
                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles)))
                .flatMap(this::handleNlpOperationSuccess);
    }
}
