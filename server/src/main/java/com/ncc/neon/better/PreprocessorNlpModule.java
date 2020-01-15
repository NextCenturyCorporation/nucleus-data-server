package com.ncc.neon.better;

import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.HashMap;

@Component
public class PreprocessorNlpModule extends NlpModule {
    private HttpEndpoint preprocessEndpoint;
    private HttpEndpoint listEndpoint;

    @Autowired
    PreprocessorNlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        super(datasetService, fileShareService, betterFileService);
    }

    public void setPreprocessEndpoint(HttpEndpoint preprocessEndpoint) {
        this.preprocessEndpoint = preprocessEndpoint;
    }

    public void setListEndpoint(HttpEndpoint listEndpoint) {
        this.listEndpoint = listEndpoint;
    }

    public Flux<RestStatus> performPreprocessing(String filename) {
        HashMap<String, String> params = new HashMap<>();
        params.put("file", filename);
        return this.performListOperation(params, listEndpoint)
                .flatMapMany(pendingFiles -> this.initPendingFiles(pendingFiles)
                .then(this.performNlpOperation(params, preprocessEndpoint))
                .doOnError(onError -> this.handleNlpOperationError(onError, pendingFiles)))
                .flatMap(this::handleNlpOperationSuccess);
    }
}
