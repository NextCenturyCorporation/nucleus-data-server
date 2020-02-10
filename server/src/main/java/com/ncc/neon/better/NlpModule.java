package com.ncc.neon.better;

import java.util.Map;

import com.ncc.neon.models.*;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

/*
Abstract class to represent a remote NLP module REST service.  These services typically expose endpoints to perform
NLP operations for preprocessing, training, and inference.
 */
public abstract class NlpModule {
    private String name;
    private WebClient client;
    private DatasetService datasetService;
    private FileShareService fileShareService;
    private BetterFileService betterFileService;

    NlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        this.datasetService = datasetService;
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
    }

    public String getName() { return this.name; }

    public void setName(String name) {
        this.name = name;
    }

    public void setClient(WebClient client) {
        this.client = client;
    }

    public abstract void setEndpoints(HttpEndpoint[] endpoints);

    protected Mono<String[]> performListOperation(Map<String, String> data, HttpEndpoint endpoint) {
        return buildRequest(data, endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String[].class);
    }

    protected Mono<?> initPendingFiles(String[] files) {
        return Mono.just(files).flatMapMany(fileList -> betterFileService.initMany(fileList))
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    protected Mono<BetterFile[]> performNlpOperation(Map<String, String> data, HttpEndpoint endpoint) {
        return buildRequest(data, endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(BetterFile[].class);
    }

    protected Mono<RestStatus> handleNlpOperationSuccess(BetterFile[] readyFiles) {
        // Set status to ready.
        for (BetterFile readyFile : readyFiles) {
            readyFile.setStatus(FileStatus.READY);
        }

        return betterFileService.upsertMany(readyFiles)
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    protected Disposable handleNlpOperationError(WebClientResponseException err, String[] pendingFiles) {
        return Flux.fromArray(pendingFiles)
                .flatMap(pendingFile -> fileShareService.delete(pendingFile)
                    .then(betterFileService.getById(pendingFile))
                    .flatMap(fileToUpdate -> {
                        // Set status of files to error.
                        fileToUpdate.setStatus(FileStatus.ERROR);
                        fileToUpdate.setStatus_message(err.getResponseBodyAsString());
                        return betterFileService.upsert(fileToUpdate);
                    }))
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                .subscribe();
    }

    protected WebClient.RequestHeadersSpec<?> buildRequest(Map<String, String> data, HttpEndpoint endpoint) {
        if (endpoint.getMethod() == HttpMethod.GET) {
            // Build Http Headers for query params.
            HttpHeaders params = new HttpHeaders();
            for (Map.Entry<String, String> entry : data.entrySet()) {
                params.add(entry.getKey(), entry.getValue());
            }

            return client.get().uri(uriBuilder -> {
                uriBuilder.pathSegment(endpoint.getPathSegment());
                uriBuilder.queryParams(params);
                return uriBuilder.build();
            });
        }

        // Otherwise, we are doing a post.
        return client.post()
                .uri(uriBuilder -> uriBuilder.pathSegment(endpoint.getPathSegment()).build())
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(data));
    }

    private Class getReturnModel(HttpEndpoint endpoint) {
        Class res = Object.class;

        switch(endpoint.getType()) {
            case LIST:
            case TRAIN_LIST:
            case INF_LIST:
            case EVAL_LIST:
                res = String[].class;
            case TRAIN:
            case INF:
            case PREPROCESS:
                res = BetterFile[].class;
            case EVAL:
                res = EvaluationResponse.class;
        }

        return res;
    }
}
