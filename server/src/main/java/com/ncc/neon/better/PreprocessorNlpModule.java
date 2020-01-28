package com.ncc.neon.better;

import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;

import java.util.HashMap;

public class PreprocessorNlpModule extends NlpModule {
    private HttpEndpoint preprocessEndpoint;
    private HttpEndpoint listEndpoint;

    public PreprocessorNlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        super(datasetService, fileShareService, betterFileService);
    }

    @Override
    public void setEndpoints(HttpEndpoint[] endpoints) {
        for (HttpEndpoint endpoint : endpoints) {
            switch (endpoint.getType()) {
                case PREPROCESS:
                    preprocessEndpoint = endpoint;
                    break;
                case LIST:
                    listEndpoint = endpoint;
                    break;
            }
        }
    }

    public Flux<RestStatus> performPreprocessing(String filename) {
        HashMap<String, String> params = new HashMap<>();
        params.put("file", filename);
        return this.performListOperation(params, listEndpoint)
                .flatMapMany(pendingFiles -> this.initPendingFiles(pendingFiles)
                .then(this.performNlpOperation(params, preprocessEndpoint)
                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles)))
                .flatMap(this::handleNlpOperationSuccess));
    }
}
