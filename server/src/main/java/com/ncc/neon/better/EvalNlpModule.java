package com.ncc.neon.better;

import com.ncc.neon.models.EvaluationOutput;
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

    public EvalNlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService, RunService runService, EvaluationService evaluationService, ModuleService moduleService) {
        super(datasetService, fileShareService, betterFileService, moduleService);
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

    public Mono<RestStatus> performEval(String refFile, String sysFile, String runId) {
        HashMap<String, String> params = new HashMap<>();
        params.put("sysfile", sysFile);

        return this.performListOperation(params, evalListEndpoint)
                .doOnError(onError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage())))
                .flatMap(pendingFiles -> this.initPendingFiles(pendingFiles)
                        .then(runService.updateOutputs(runId, "eval_outputs", pendingFiles))
                        .flatMap(response -> {
                            params.put("reffile", refFile);
                            return this.performEvalOperation(params, evalEndpoint)
                                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles))
                                .flatMap(res -> runService.updateToDoneStatus(runId, res.getOverallScore())
                                        .flatMap(ignored -> {
                                            EvaluationOutput evaluationOutput = new EvaluationOutput(runId, res.getEvaluation());
                                            return evaluationService.insert(evaluationOutput)
                                                    .flatMap(evaluation -> handleNlpOperationSuccess(res.getFiles()));
                                        }));
                        }));
    }
}
