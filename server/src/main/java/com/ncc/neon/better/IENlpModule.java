package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

@Component
public class IENlpModule extends NlpModule {
    private HttpEndpoint trainEndpoint;
    private HttpEndpoint trainListEndpoint;
    private HttpEndpoint infEndpoint;
    private HttpEndpoint infListEndpoint;

    @Autowired
    IENlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        super(datasetService, fileShareService, betterFileService);
    }

    @Override
    protected void setEndpoints(HttpEndpoint[] endpoints) {
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

    public Flux<RestStatus> performTraining(String listConfigFile, String trainConfigFile) throws IOException {
        String shareDir = System.getenv("SHARE_DIR");
        File listConfig = new File(Paths.get(shareDir, listConfigFile).toString());
        File trainConfig = new File(Paths.get(shareDir, trainConfigFile).toString());
        Map<String, String> listConfigMap = new ObjectMapper().readValue(listConfig, Map.class);
        Map<String, String> trainConfigMap = new ObjectMapper().readValue(trainConfig, Map.class);

        return this.performListOperation(listConfigMap, trainListEndpoint)
                .doOnError(onError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage())))
                .flatMapMany(pendingFiles -> this.initPendingFiles(pendingFiles)
                .then(this.performNlpOperation(trainConfigMap, trainEndpoint))
                .doOnError(onError -> this.handleNlpOperationError(onError, pendingFiles)))
                .flatMap(this::handleNlpOperationSuccess);
    }
}
