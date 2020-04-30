package com.ncc.neon.better;

import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.ModuleService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.core.env.Environment;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.util.HashMap;

public class IRNlpModule extends NlpModule {

    private HttpEndpoint queryEndpoint;

    public IRNlpModule(NlpModuleModel moduleModel, DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService, ModuleService moduleService, Environment env) {
        super(moduleModel, datasetService, fileShareService, betterFileService, moduleService, env);
    }

    @Override
    protected void initEndpoints(HttpEndpoint[] endpoints) {
        super.initEndpoints(endpoints);
        for (HttpEndpoint endpoint : endpoints) {
            switch (endpoint.getType()) {
                case IR:
                    queryEndpoint = endpoint;
            }
        }
    }

    public Mono<RestStatus> searchIR(String query) {
        HashMap<String, String> params = new HashMap<>();
        params.put("query", query);
        return this.performNlpOperation(params, queryEndpoint)
                .doOnError(onError -> this.handleNlpOperationError((WebClientResponseException) onError, pendingFiles)))
                .flatMap(this::handleNlpOperationSuccess);
    }

}