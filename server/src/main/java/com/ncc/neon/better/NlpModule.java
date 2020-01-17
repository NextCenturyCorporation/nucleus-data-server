package com.ncc.neon.better;

import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

public abstract class NlpModule {
    private String name;
    private WebClient client;
    private HttpEndpoint[] endpoints;
    private DatasetService datasetService;
    private FileShareService fileShareService;
    private BetterFileService betterFileService;

    NlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        this.datasetService = datasetService;
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setClient(WebClient client) {
        this.client = client;
    }

    protected abstract void setEndpoints(HttpEndpoint[] endpoints);

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

    protected Disposable handleNlpOperationError(Throwable err, String[] pendingFiles) {
        return Flux.fromArray(pendingFiles)
                .flatMap(pendingFile -> fileShareService.delete(pendingFile)
                    .then(betterFileService.getById(pendingFile))
                    .flatMap(fileToUpdate -> {
                        // Set status of files to error.
                        fileToUpdate.setStatus(FileStatus.ERROR);
                        fileToUpdate.setStatus_message(err.getMessage());
                        return betterFileService.upsert(fileToUpdate);
                    }))
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                .subscribe();
    }

    private WebClient.RequestHeadersSpec<?> buildRequest(Map<String, String> data, HttpEndpoint endpoint) {
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

        // Build multivalue map for BodyInserters.
        LinkedMultiValueMap<String, String> multiValueMap = new LinkedMultiValueMap<>();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            multiValueMap.add(entry.getKey(), entry.getValue());
        }

        return client.post()
                .uri(uriBuilder -> uriBuilder.pathSegment(endpoint.getPathSegment()).build())
                .body(BodyInserters.fromFormData(multiValueMap));
    }
}
