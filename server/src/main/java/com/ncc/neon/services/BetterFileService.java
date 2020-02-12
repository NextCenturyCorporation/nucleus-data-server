package com.ncc.neon.services;

import com.ncc.neon.models.BetterFile;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class BetterFileService extends ElasticSearchService<BetterFile> {
    private static final String index = "file";
    private static final String dataType = "file";

    @Autowired
    private DatasetService datasetService;

    @Autowired
    BetterFileService(DatasetService datasetService, @Value("${db_host}") String dbHost) {
        super(dbHost, index, dataType, BetterFile.class, datasetService);
    }

    public Mono<RestStatus> initMany(String[] filesToAdd) {
        return Flux.fromArray(filesToAdd)
                .flatMap(fileToAdd -> upsertAndRefresh(new BetterFile(fileToAdd, 0), fileToAdd))
                .then(Mono.just(RestStatus.OK));
    }
}
