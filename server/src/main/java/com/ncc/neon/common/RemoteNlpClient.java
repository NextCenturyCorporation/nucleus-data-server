package com.ncc.neon.common;

import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public class RemoteNlpClient {
    private WebClient remoteClient;
    private DatasetService datasetService;
    private FileShareService fileShareService;
    private BetterFileService betterFileService;

    public RemoteNlpClient(WebClient client, DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        remoteClient = client;
        this.datasetService = datasetService;
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
    }

    public Mono<String[]> getOutputFileList(HttpHeaders file) {
        return this.remoteClient.get()
                .uri(uriBuilder -> {
                    uriBuilder.pathSegment("list");
                    uriBuilder.queryParams(file);
                    return uriBuilder.build();
                })
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String[].class);
    }

    public Mono<RestStatus> performNlpOperation(HttpHeaders operationData, Mono<String[]> pendingFiles) {
        return pendingFiles
                .flatMapMany(fileList -> betterFileService.initMany(fileList)
                        .then(betterFileService.refreshFilesIndex().retry(3))
                        .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                        .then(remoteClient.get()
                                .uri(uriBuilder ->
                                        uriBuilder.queryParams(operationData).build())
                                .accept(MediaType.APPLICATION_JSON)
                                .retrieve()
                                .bodyToMono(BetterFile[].class))
                        .doOnError(onError -> {
                            Flux.fromArray(fileList)
                                    .flatMap(filename -> fileShareService.delete(filename)
                                            .then(betterFileService.getById(filename))
                                            .flatMap(fileToUpdate -> {
                                                // Set status of files to error.
                                                fileToUpdate.setStatus(FileStatus.ERROR);
                                                fileToUpdate.setStatus_message(onError.getMessage());
                                                return betterFileService.upsert(fileToUpdate);
                                            }))
                                    .then(betterFileService.refreshFilesIndex().retry(3))
                                    .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                                    .subscribe();
                        }))
                .flatMap(readyFiles -> {
                    // Set status to ready.
                    for (BetterFile readyFile : readyFiles) {
                        readyFile.setStatus(FileStatus.READY);
                    }

                    return betterFileService.upsertMany(readyFiles);
                })
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }
}
