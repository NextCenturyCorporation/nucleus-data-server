package com.ncc.neon.services;

import com.ncc.neon.models.Docfile;
import com.ncc.neon.util.DateUtil;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class IRDataService extends ElasticSearchService<Docfile> {
    @Autowired
    private DatasetService datasetService;
    public static final String STATUS_FIELD = "status";

    @Autowired
    IRDataService(DatasetService datasetService,
                      @Value("${db_host}") String dbHost,
                      @Value("${file.table}") String fileTable) {
        super(dbHost, fileTable, fileTable, Docfile.class, datasetService);
    }

    public Mono<RestStatus> initFile(String fileToAdd) {
        Docfile docfileToAdd = new Docfile(fileToAdd, 0);
        // docfileToAdd.setTimestamp(DateUtil.getCurrentDateTime());
        return upsertAndRefresh(docfileToAdd, fileToAdd);
    }

    public Mono<RestStatus> initMany(String[] filesToAdd) {
        return Flux.fromArray(filesToAdd)
                .flatMap(this::initFile)
                .then(Mono.just(RestStatus.OK));
    }
}
