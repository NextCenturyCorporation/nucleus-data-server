package com.ncc.neon.common;

import com.ncc.neon.models.BetterFile;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

public class BetterFileMachineTrainer extends BetterFileOperationHandler {
    public BetterFileMachineTrainer(WebClient remoteClient) {
        super(remoteClient);
    }

    public Mono<String[]> getOutputFileList(String filePrefix) {
        return this.remoteClient.get()
                .uri(uriBuilder -> {
                    uriBuilder.pathSegment("list");
                    uriBuilder.queryParam("file_prefix", filePrefix);
                    return uriBuilder.build();
                })
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String[].class);
    }

    public Mono<BetterFile[]> performTrainingPreprocessing(String trainSrc, String trainTgt, String validSrc, String validTgt, String outputPrefix) {
        return this.remoteClient.get()
                .uri(uriBuilder ->
                        uriBuilder.queryParam("train_src", trainSrc)
                                .queryParam("train_tgt", trainTgt)
                                .queryParam("valid_src", validSrc)
                                .queryParam("valid_tgt", validTgt)
                                .queryParam("output_basename", outputPrefix).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(BetterFile[].class);
    }
}
