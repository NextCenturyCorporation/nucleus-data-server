package com.ncc.neon.services;

import com.ncc.neon.models.Docfile;
import com.ncc.neon.util.DateUtil;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.*;

@Component
public class IRDataService extends ElasticSearchService<Docfile> {
    @Autowired
    private DatasetService datasetService;

    @Autowired
    IRDataService(DatasetService datasetService,
                      @Value("${db_host}") String dbHost,
                      @Value("${file.table}") String fileTable) {
        super(dbHost, fileTable, fileTable, Docfile.class, datasetService);
    }

    /*
    public Mono<RestStatus> initMany(HashMap[] filesToAdd) {
        return Flux.fromArray(filesToAdd)
                .flatMap(this::initFile)
                .then(Mono.just(RestStatus.OK));
    }
    */

    public Flux<Docfile> getIRDocResponse(String index, String type, String[] searchIDs) {
        Flux<Docfile> docList = Flux.just();

        return Flux.create(sink -> {
            for(String id : searchIDs) {
                docList.flatMap(x -> this.getByDocId(index, type, id));
//                Flux<Docfile> returnList = Flux.just(this.getByDocId(index, type, id));
                //docList = Flux.concat(docList, returnList);
            }
        });

//        return docList.flatMap(x -> this.getByDocId(index, type, id));
    }
    
}
