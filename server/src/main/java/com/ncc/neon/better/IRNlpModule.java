package com.ncc.neon.better;

import com.ncc.neon.models.EndpointType;
import com.ncc.neon.models.NlpModuleModel;
import com.ncc.neon.models.RelevanceJudgement;
import com.ncc.neon.models.IRResponse;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.ModuleService;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IRNlpModule extends NlpModule {

    private HttpEndpoint irEndpoint;
    private HttpEndpoint irieEndpoint;
    private HttpEndpoint docfileEndpoint;
    private HttpEndpoint retrofitterEndpoint;
    private HttpEndpoint startEndpoint;
    private HttpEndpoint rankerEndpoint;
    private HashMap<String, String> params;

    public IRNlpModule(NlpModuleModel moduleModel, FileShareService fileShareService, BetterFileService betterFileService, ModuleService moduleService, Environment env) {
        // does not need runservice variable. 
        super(moduleModel, fileShareService, betterFileService, moduleService, env);
    }

    @Override
    protected void initEndpoints(HttpEndpoint[] endpoints) {
        super.initEndpoints(endpoints);
        this.params = new HashMap<>();
        for (HttpEndpoint endpoint : endpoints) {
            switch (endpoint.getType()) {
                case IR:
                    irEndpoint = endpoint;
                case DOCFILE:
                    docfileEndpoint = endpoint;
                case RETROFITTER:
                    retrofitterEndpoint = endpoint;
                case START:
                    startEndpoint = endpoint;
                case RANKER:
                    rankerEndpoint = endpoint;
                case IRIE:
                    irieEndpoint = endpoint;
            }
        }
    }

    @Override
    protected Mono<Object> handleNlpOperationSuccess(ClientResponse nlpResponse) {
        // Assume doc ids were returned.
        return nlpResponse.bodyToMono(IRResponse.class);
    }

    public Mono<IRResponse> searchIR(String query) {
        this.params.put("query", query);
        return this.performNlpOperation(this.params, irEndpoint).cast(IRResponse.class);
    }

    public Mono<IRResponse> searchIRPost(String query) {
        this.params.put("query", query);
        // TODO - How do you actually update the IR endpoint?
        // Changing the HTTP method in the json file doesnt seem to work
        HttpEndpoint irPost = new HttpEndpoint(irEndpoint.getPathSegment(), HttpMethod.POST, EndpointType.IR);
        return this.performNlpOperation(this.params, irPost).cast(IRResponse.class);
    }

    public Mono<IRResponse> runIrRequest(Map<String, Object> request) {
        return this.performPostNlpOperation(request, new HttpEndpoint("/request", HttpMethod.POST, EndpointType.IR)).cast(IRResponse.class);
    }

    /**
     * Attempt to support starting the whole IR-IE process from the frontend
     */
    public Mono<IRResponse> irie(String query) {
        this.params.put("query", query);
        return this.performNlpOperation(this.params, irEndpoint).cast(IRResponse.class);
    }

    //build a query for the docfile flask endpoint.
    /**
     * Deprecated. 2020 attempt to hook HITL UI with IR
     */
    public Mono<String> getDocfile() {
        HashMap<String, String> params = new HashMap<>();
        return this.performNlpOperation(this.params, docfileEndpoint).cast(String.class);
    }

    @Override
    protected Map<String, String> getListEndpointParams(String filePrefix) {
        // This Module does not have a list endpoint. 
        return null;
    }

    /**
     * Deprecated. 2020 attempt to hook HITL UI with IR
     */
    public Mono<IRResponse> retrofit(ArrayList<RelevanceJudgement> rels) {
        this.params.put("rels", rels.toString());
        return this.performNlpOperation(this.params, retrofitterEndpoint).cast(IRResponse.class);
    }

    public Mono<Object> start(Object eval){
//        this.params.put("mode", eval.mode);
        return this.performNlpOperation(this.params, startEndpoint);
    }

    /**
     * Deprecated. 2020 attempt to hook HITL UI with IR
     */
    public Mono<Object> ranker(){
        return this.performNlpOperation(this.params, rankerEndpoint);
    }

}