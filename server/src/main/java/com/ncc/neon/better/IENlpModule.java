package com.ncc.neon.better;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.rest.RestStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.RunService;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class IENlpModule extends NlpModule {
    private HttpEndpoint trainEndpoint;
    private HttpEndpoint trainListEndpoint;
    private HttpEndpoint infEndpoint;
    private HttpEndpoint infListEndpoint;
    private RunService runService;
    private Path shareDir;

    public IENlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService, RunService runService) {
        super(datasetService, fileShareService, betterFileService);
        this.runService = runService;
        shareDir = fileShareService.getSharePath();
    }

    @SuppressWarnings("incomplete-switch")
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

    public Mono<RestStatus> performTraining(String trainConfigFile, String runId) {
        // Parse JSON file to maps.
        File trainConfig = new File(Paths.get(shareDir.toString(), trainConfigFile).toString());
        try {
            Map<String, String> trainConfigMap = new ObjectMapper().readValue(trainConfig, Map.class);
            Map<String, String> listConfigMap = new HashMap<>();
            // Reuse output_file_prefix field for the list call.
            listConfigMap.put("output_file_prefix", trainConfigMap.get("output_file_prefix"));

            return performListOperation(listConfigMap, trainListEndpoint)
                    .doOnError(onError -> handleErrorDuringRun(onError, runId))
                    .flatMap(pendingFiles -> initPendingFiles(pendingFiles)
                            .then(runService.updateOutputs(runId, "train_outputs", pendingFiles))
                            .flatMap(initRes -> performNlpOperation(trainConfigMap, trainEndpoint)
                                    .flatMap(this::handleNlpOperationSuccess)
                                    .doOnError(onError -> {
                                        handleNlpOperationError((WebClientResponseException) onError, pendingFiles);
                                        handleErrorDuringRun(onError, runId);
                                    })));
        }
        catch (IOException e) {
            return Mono.error(e);
        }
    }

    public Mono<RestStatus> performInference(String infConfigFile, String runId) {
        File infConfig = new File(Paths.get(shareDir.toString(), infConfigFile).toString());
        Map<String, String> infConfigMap;
        try {
            infConfigMap = new ObjectMapper().readValue(infConfig, Map.class);
        }
        catch (IOException e) {
            return Mono.error(e);
        }
        Map<String, String> listConfigMap = new HashMap<>();
        // Reuse output_file_prefix field for the list call.
        listConfigMap.put("inf_file", infConfigMap.get("inf_file"));

        return performListOperation(listConfigMap, infListEndpoint)
                .doOnError(onError -> handleErrorDuringRun(onError, runId))
                .flatMap(pendingFiles -> initPendingFiles(pendingFiles)
                        .then(runService.updateOutputs(runId, "inf_outputs", pendingFiles))
                        .then(performNlpOperation(infConfigMap, infEndpoint)
                        .flatMap(this::handleNlpOperationSuccess)
                                .doOnError(onError -> {
                                    handleNlpOperationError((WebClientResponseException) onError, pendingFiles);
                                    handleErrorDuringRun(onError, runId);
                                })));

    }

    private Disposable handleErrorDuringRun(Throwable err, String runId) {
        return runService.updateToErrorStatus(runId, err.getMessage())
                .then(runService.refreshIndex())
                .subscribe();
    }
}
