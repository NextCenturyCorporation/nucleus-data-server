package com.ncc.neon.better;

import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.ModuleService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;

public class PreprocessorNlpModule extends NlpModule {
    private HttpEndpoint preprocessEndpoint;
    private HttpEndpoint listEndpoint;

    public PreprocessorNlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService, ModuleService moduleService) {
        super(datasetService, fileShareService, betterFileService, moduleService);
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

    public Mono<RestStatus> performPreprocessing(String filename) {
        HashMap<String, String> params = new HashMap<>();
        params.put("file", filename);
        return this.performListOperation(params, listEndpoint)
                .flatMap(pendingFiles -> this.initPendingFiles(pendingFiles)
                .then(this.performNlpOperation(params, preprocessEndpoint)
                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles)))
                .flatMap(this::handleNlpOperationSuccess));
    }
}
