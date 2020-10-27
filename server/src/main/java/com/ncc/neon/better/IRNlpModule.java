package com.ncc.neon.better;

import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.models.RelevanceJudgement;
import com.ncc.neon.models.IRResponse;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.ModuleService;
import org.springframework.core.env.Environment;
import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IRNlpModule extends NlpModule {

    private HttpEndpoint irEndpoint;
    private HttpEndpoint docfileEndpoint;
    private HttpEndpoint retrofitterEndpoint;

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
                    irEndpoint = endpoint;
                case DOCFILE:
                    docfileEndpoint = endpoint;
                case RETROFITTER:
                    retrofitterEndpoint = endpoint;
            }
        }
    }

    @Override
    protected Mono<Object> handleNlpOperationSuccess(ClientResponse nlpResponse) {
        // Assume doc ids were returned.
        return nlpResponse.bodyToMono(IRResponse.class);
    }

    public Mono<IRResponse> searchIR(String query) {
        HashMap<String, String> params = new HashMap<>();
        params.put("query", query);
        return this.performNlpOperation(params, irEndpoint).cast(IRResponse.class);
    }

    //build a query for the docfile flask endpoint.
    public Mono<String> getDocfile() {
        HashMap<String, String> params = new HashMap<>();
        return this.performNlpOperation(params, docfileEndpoint).cast(String.class);
    }

    @Override
    protected Map<String, String> getListEndpointParams(String filePrefix) {
        // This Module does not have a list endpoint. 
        return null;
    }

    public Mono<IRResponse> retrofit(ArrayList<RelevanceJudgement> rels) {
        HashMap<String, String> params = new HashMap<>();
        params.put("rels", rels.toString());
        return this.performNlpOperation(params, retrofitterEndpoint).cast(IRResponse.class);
    }

}