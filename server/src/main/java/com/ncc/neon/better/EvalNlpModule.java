package com.ncc.neon.better;

import com.ncc.neon.models.EvaluationResponse;
import com.ncc.neon.services.*;
import org.elasticsearch.rest.RestStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class EvalNlpModule extends NlpModule {
    private HttpEndpoint evalEndpoint;
    private HttpEndpoint evalListEndpoint;
    private RunService runService;
    private EvaluationService evaluationService;

    public EvalNlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService, RunService runService, EvaluationService evaluationService) {
        super(datasetService, fileShareService, betterFileService);
        this.runService = runService;
        this.evaluationService = evaluationService;
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

    protected Mono<EvaluationResponse> performEvalOperation(Map<String, String> data, HttpEndpoint endpoint) {
        return buildRequest(data, endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(EvaluationResponse.class);
    }

    public Flux<RestStatus> performEval(String refFile, String sysFile, String runId) {
        HashMap<String, String> params = new HashMap<>();

        return this.performListOperation(params, evalListEndpoint)
                .doOnError(onError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage())))
                .flatMapMany(pendingFiles -> this.initPendingFiles(pendingFiles)
                        .then(runService.updateOutputs(runId, "eval_outputs", pendingFiles))
                        .flatMapMany(response -> {
                            params.put(refFile, refFile);
                            return this.performEvalOperation(params, evalEndpoint)
                                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles))
                                .flatMapMany(res -> evaluationService.insert(res.getEvaluationOutput())
                                .flatMap(evaluation -> handleNlpOperationSuccess(res.getOutputFiles())));
                        }));
    }
}
