package com.ncc.neon.better;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

import com.ncc.neon.models.*;
import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.ModuleService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


/*
Abstract class to represent a remote NLP module REST service.  These services typically expose endpoints to perform
NLP operations for preprocessing, training, and inference.
 */
public abstract class NlpModule {
    private String name;
    private HttpEndpoint statusEndpoint;
    private WebClient client;
    private DatasetService datasetService;
    private FileShareService fileShareService;
    private BetterFileService betterFileService;
    private ModuleService moduleService;
    private Environment env;

    NlpModule(NlpModuleModel moduleModel, DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService, ModuleService moduleService, Environment env) {
        this.datasetService = datasetService;
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
        this.moduleService = moduleService;
        this.env = env;
        name = moduleModel.getName();
        client = buildNlpWebClient(name);
        initEndpoints(moduleModel.getEndpoints());
    }

    public String getName() { return this.name; }

    public void setName(String name) {
        this.name = name;
    }

    public Mono<HttpStatus> getRemoteStatus() {
        return buildRequest(new HashMap<>(), statusEndpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(HttpStatus.class)
                .doOnSuccess(res -> moduleService.setStatusToActive(name).subscribe())
                .doOnError(this::handleHttpError);
    }

    protected void initEndpoints(HttpEndpoint[] endpoints) {
        for (HttpEndpoint endpoint : endpoints) {
            if (endpoint.getType() == EndpointType.STATUS) {
                statusEndpoint = endpoint;
            }
        }
    };

    protected Mono<String[]> performListOperation(Map<String, String> data, HttpEndpoint endpoint) {
        return buildRequest(data, endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String[].class)
                .doOnError(this::handleHttpError);
    }

    protected Mono<?> initPendingFiles(String[] files) {
        return Mono.just(files).flatMap(fileList -> betterFileService.initMany(fileList));
    }

    protected Mono<BetterFile[]> performNlpOperation(Map<String, String> data, HttpEndpoint endpoint) {
        return buildRequest(data, endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(BetterFile[].class)
                .doOnError(this::handleHttpError);
    }

    protected Mono<RestStatus> handleNlpOperationSuccess(BetterFile[] readyFiles) {
        // Set status to ready.
        for (BetterFile readyFile : readyFiles) {
            readyFile.setStatus(FileStatus.READY);
        }

        return Flux.fromArray(readyFiles)
                .flatMap(readyFile -> betterFileService.upsertAndRefresh(readyFile, readyFile.getFilename()))
                .then(Mono.just(RestStatus.OK));
    }

    protected Disposable handleNlpOperationError(WebClientResponseException err, String[] pendingFiles) {
        return Flux.fromArray(pendingFiles)
                .flatMap(pendingFile -> fileShareService.delete(pendingFile)
                    .then(betterFileService.getById(pendingFile))
                    .flatMap(fileToUpdate -> {
                        // Set status of files to error.
                        fileToUpdate.setStatus(FileStatus.ERROR);
                        fileToUpdate.setStatus_message(err.getResponseBodyAsString());
                        return betterFileService.upsertAndRefresh(fileToUpdate, fileToUpdate.getFilename());
                    }))
                .subscribe();
    }

    protected WebClient.RequestHeadersSpec<?> buildRequest(Map<String, String> data, HttpEndpoint endpoint) {
        if (endpoint.getMethod() == HttpMethod.GET) {
            // Build Http Headers for query params.
            HttpHeaders params = new HttpHeaders();
            for (Map.Entry<String, String> entry : data.entrySet()) {
                params.add(entry.getKey(), entry.getValue());
            }

            return client.get().uri(uriBuilder -> {
                uriBuilder.pathSegment(endpoint.getPathSegment());
                uriBuilder.queryParams(params);
                return uriBuilder.build();
            });
        }

        // Otherwise, we are doing a post.
        return client.post()
                .uri(uriBuilder -> uriBuilder.pathSegment(endpoint.getPathSegment()).build())
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(data));
    }

    private WebClient buildNlpWebClient(String name) {
        String url = "http://";
        String host = System.getenv().getOrDefault(name.toUpperCase() + "_HOST", "localhost");
        String port = env.getProperty(name + ".port");

        url += host + ":" + port;

        return WebClient.create(url);
    }

    private Throwable handleHttpError(Throwable err) {
        if (err instanceof ConnectException) {
            moduleService.setStatusToDown(name).subscribe();
        }

        return err;
    }
}
