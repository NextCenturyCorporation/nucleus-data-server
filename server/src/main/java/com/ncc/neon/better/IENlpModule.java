package com.ncc.neon.better;

import java.util.HashMap;
import java.util.Map;

import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.services.*;
import org.elasticsearch.rest.RestStatus;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class IENlpModule extends NlpModule {
    private final String TRAIN_OUTPUTS_KEY = "train_outputs";
    private final String INF_OUTPUTS_KEY = "inf_outputs";
    private final String JOB_ID_KEY = "job_id";

    private HttpEndpoint trainEndpoint;
    private HttpEndpoint trainListEndpoint;
    private HttpEndpoint infEndpoint;
    private HttpEndpoint infListEndpoint;
    private HttpEndpoint cancelEndpoint;
    private RunService runService;

    public IENlpModule(NlpModuleModel moduleModel, FileShareService fileShareService, BetterFileService betterFileService, RunService runService, ModuleService moduleService, Environment env) {
        super(moduleModel, fileShareService, betterFileService, moduleService, env);
        this.runService = runService;
    }

    @Override
    protected Map<String, String> getListEndpointParams(String filePrefix) {
        Map<String, String> res = new HashMap<>();
        res.put("output_file_prefix", filePrefix);
        return res;
    }

    @Override
    protected Mono<RestStatus> handleNlpOperationSuccess(ClientResponse nlpResponse) {
        HttpStatus responseStatus = nlpResponse.statusCode();

        // Check if nlp operation was canceled.
        if (responseStatus == HttpStatus.RESET_CONTENT) {
            return nlpResponse.bodyToMono(String.class).flatMap(runService::updateToCanceledStatus);
        }

        // Assume files were returned otherwise.
        return nlpResponse.bodyToMono(BetterFile[].class).flatMap(this::updateFilesToReady);
    }

    @SuppressWarnings("incomplete-switch")
	@Override
    protected void initEndpoints(HttpEndpoint[] endpoints) {
        super.initEndpoints(endpoints);
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
                case CANCEL:
                    cancelEndpoint = endpoint;
                    break;
            }
        }
    }

    public Mono<Object> performTraining(EvalConfig config, String runId) {
        return performListOperation(config.outputFilePrefix, trainListEndpoint)
                .doOnError(onError -> handleErrorDuringRun(onError, runId))
                .flatMap(pendingFiles -> initPendingFiles(pendingFiles)
                        .then(runService.updateOutputs(runId, TRAIN_OUTPUTS_KEY, pendingFiles))
                        .flatMap(initRes -> performNlpOperation(config.trainConfigParams, trainEndpoint)
                                .doOnError(onError -> {
                                    handleNlpOperationError((WebClientResponseException) onError, pendingFiles);
                                    handleErrorDuringRun(onError, runId);
                                })));
    }

    public Mono<Object> performInference(EvalConfig config, String runId) {
        return runService.isCanceled(runId).flatMap(isCanceled -> {
            if (isCanceled) {
                return Mono.empty();
            }

            return runService.updateToInferenceStatus(runId)
                    .then(performListOperation(config.outputFilePrefix, infListEndpoint)
                            .doOnError(onError -> handleErrorDuringRun(onError, runId))
                            .flatMap(pendingFiles -> initPendingFiles(pendingFiles)
                                    .then(runService.updateOutputs(runId, INF_OUTPUTS_KEY, pendingFiles))
                                    .flatMap(ignored -> performNlpOperation(config.infConfigParams, infEndpoint)
                                            .doOnError(infError -> handleNlpOperationError((WebClientResponseException) infError, pendingFiles)))));
        });
    }

    public Mono<String> cancelEval(String runId) {
        Map<String, String> cancelParams = new HashMap();
        cancelParams.put(JOB_ID_KEY, runId);

        return buildRequest(cancelParams, cancelEndpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String.class)
                .doOnSuccess(res -> runService.updateToCanceledStatus(runId).subscribe());
    }

    private Disposable handleErrorDuringRun(Throwable err, String runId) {
        return runService.updateToErrorStatus(runId, err.getMessage())
                .then(runService.refreshIndex())
                .subscribe();
    }
}
