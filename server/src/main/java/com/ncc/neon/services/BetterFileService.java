package com.ncc.neon.services;

import com.ncc.neon.models.BetterFile;
import com.ncc.neon.util.DateUtil;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class BetterFileService extends ElasticSearchService<BetterFile> {
    @Autowired
    private DatasetService datasetService;

    @Autowired
    BetterFileService(DatasetService datasetService,
                      @Value("${db_host}") String dbHost,
                      @Value("${file.table}") String fileTable) {
        super(dbHost, fileTable, fileTable, BetterFile.class, datasetService);
    }

    public Mono<RestStatus> initFile(String fileToAdd) {
        BetterFile betterFileToAdd = new BetterFile(fileToAdd, 0);
        betterFileToAdd.setTimestamp(DateUtil.getCurrentDateTime());
        return upsertAndRefresh(betterFileToAdd, fileToAdd);
    }

    public Mono<RestStatus> initMany(String[] filesToAdd) {
        return Flux.fromArray(filesToAdd)
                .flatMap(fileToAdd -> {
                    return initFile(fileToAdd);
                })
                .then(Mono.just(RestStatus.OK));
    }
}
