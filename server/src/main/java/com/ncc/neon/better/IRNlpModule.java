package com.ncc.neon.better;

import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.ModuleService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.core.env.Environment;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class IRNlpModule extends NlpModule {

    private HttpEndpoint queryEndpoint;

    public IRNlpModule(NlpModuleModel moduleModel, FileShareService fileShareService, BetterFileService betterFileService, ModuleService moduleService, Environment env) {
        // does not need runservice variable. 
        super(moduleModel, fileShareService, betterFileService, moduleService, env);
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

    @Override
    protected Mono<Object> handleNlpOperationSuccess(ClientResponse nlpResponse) {
        // Assume doc ids were returned.
        return nlpResponse.bodyToMono(String[].class);
    }

    public Mono<Object> searchIR(String query) {
        HashMap<String, String> params = new HashMap<>();
        params.put("query", query);
        return this.performNlpOperation(params, queryEndpoint);
    }

    //build a query for the docfile flask endpoint.
    


    @Override
    protected Map<String, String> getListEndpointParams(String filePrefix) {
        // This Module does not have a list endpoint. 
        return null;
    }

}