package com.ncc.neon.better;

import java.util.HashMap;

import org.elasticsearch.rest.RestStatus;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;

import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;

import reactor.core.publisher.Flux;

public class EvalNlpModule extends NlpModule {
    private HttpEndpoint evalEndpoint;
    private HttpEndpoint evalListEndpoint;

    public EvalNlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        super(datasetService, fileShareService, betterFileService);
    }

    @Override
    public void setEndpoints(HttpEndpoint[] endpoints) {
        for (HttpEndpoint endpoint : endpoints) {
            switch (endpoint.getType()) {
                case EVAL:
                    evalEndpoint = endpoint;
                    break;
                case EVAL_LIST:
                    evalListEndpoint = endpoint;
                    break;
            }
        }
    }

    public Flux<RestStatus> performEval(String refFile, String sysFile) {
        HashMap<String, String> params = new HashMap<>();
        params.put("reffile", refFile);
        params.put("sysfile", sysFile);

        return this.performListOperation(params, evalListEndpoint)
                .doOnError(onError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage())))
                .flatMapMany(pendingFiles -> this.initPendingFiles(pendingFiles)
                .then(this.performNlpOperation(params, evalEndpoint))
                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles))
                .flatMap(this::handleNlpOperationSuccess));
    }
}
