package com.ncc.neon.common;

import com.ncc.neon.models.BetterFile;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

public abstract class BetterFileOperationHandler {
    protected WebClient remoteClient;

    BetterFileOperationHandler(WebClient remoteClient) {
        this.remoteClient = remoteClient;
    }

    public Mono<String[]> getOutputFileList(String filename) {
        return this.remoteClient.get()
                .uri(uriBuilder -> {
                    uriBuilder.pathSegment("list");
                    uriBuilder.queryParam("file", filename);
                    return uriBuilder.build();
                })
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String[].class);
    }
    public Mono<BetterFile[]> performFileOperation(String inputFile) {
        return this.remoteClient.get()
                .uri(uriBuilder ->
                        uriBuilder.queryParam("file", inputFile).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(BetterFile[].class);
    }
}
