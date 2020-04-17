package com.ncc.neon.better;

import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.EvaluationOutput;
import com.ncc.neon.models.EvaluationResponse;
import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.services.*;
import org.elasticsearch.rest.RestStatus;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class EvalNlpModule extends NlpModule {
    private final String EVAL_OUTPUTS_KEY = "eval_outputs";

    private HttpEndpoint evalEndpoint;
    private HttpEndpoint evalListEndpoint;
    private RunService runService;
    private EvaluationService evaluationService;

    public EvalNlpModule(NlpModuleModel moduleModel, FileShareService fileShareService, BetterFileService betterFileService, RunService runService, EvaluationService evaluationService, ModuleService moduleService, Environment env) {
        super(moduleModel, fileShareService, betterFileService, moduleService, env);
        this.runService = runService;
        this.evaluationService = evaluationService;
    }

    @Override
    protected Map<String, String> getListEndpointParams(String filePrefix) {
        HashMap<String, String> res = new HashMap<>();
        res.put("sysfile", filePrefix);
        return res;
    }

    @Override
    protected Mono<RestStatus> handleNlpOperationSuccess(ClientResponse nlpResponse) {
        return nlpResponse.bodyToMono(BetterFile[].class).flatMap(this::updateFilesToReady);
    }

    @Override
    protected void initEndpoints(HttpEndpoint[] endpoints) {
        super.initEndpoints(endpoints);
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
        params.put("reffile", refFile);

        return this.performListOperation(sysFile, evalListEndpoint)
                .doOnError(onError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage())))
                .flatMap(pendingFiles -> this.initPendingFiles(pendingFiles)
                        .then(runService.updateOutputs(runId, EVAL_OUTPUTS_KEY, pendingFiles))
                        .flatMap(response -> this.performEvalOperation(params, evalEndpoint)
                            .doOnError(onError -> {
                                handleNlpOperationError((WebClientResponseException) onError, pendingFiles);
                                handleErrorDuringRun(onError, runId);
                            })
                            .flatMap(res -> runService.updateToDoneStatus(runId, res.getOverallScore())
                                    .flatMap(ignored -> {
                                        EvaluationOutput evaluationOutput = new EvaluationOutput(runId, res.getEvaluation());
                                        return evaluationService.insert(evaluationOutput).then(Mono.just(RestStatus.OK));
//                                                .flatMap(evaluation -> handleNlpOperationSuccess(res.getFiles()));
                                    }))));
    }

    public Disposable handleErrorDuringRun(Throwable err, String runId) {
        return runService.updateToErrorStatus(runId, err.getMessage())
                .then(runService.refreshIndex())
                .subscribe();
    }
}
