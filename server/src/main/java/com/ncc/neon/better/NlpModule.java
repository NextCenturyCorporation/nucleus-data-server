package com.ncc.neon.better;

import com.ncc.neon.exception.InvalidConfigDataTypeException;
import com.ncc.neon.models.*;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.FileShareService;
import com.ncc.neon.services.ModuleService;
import org.elasticsearch.rest.RestStatus;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;


/*
Abstract class to represent a remote NLP module REST service.  These services typically expose endpoints to perform
NLP operations for preprocessing, training, and inference.
 */
public abstract class NlpModule {
    private String name;
    private HttpEndpoint statusEndpoint;
    private WebClient client;
    private FileShareService fileShareService;
    private BetterFileService betterFileService;
    private ModuleService moduleService;
    private Environment env;

    NlpModule(NlpModuleModel moduleModel, FileShareService fileShareService, BetterFileService betterFileService, ModuleService moduleService, Environment env) {
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
        this.moduleService = moduleService;
        this.env = env;
        name = moduleModel.getName();
        client = buildNlpWebClient(name);
        initEndpoints(moduleModel.getEndpoints());
    }

    protected abstract Map<String, String> getListEndpointParams(String filePrefix);

    /*
    Each NLP module may have their own way for dealing with the response of their operation, so
    let the concrete classes implement this handling logic.
     */
    protected abstract Mono<Object> handleNlpOperationSuccess(ClientResponse nlpResponse);

    public String getName() { return this.name; }

    public void setName(String name) {
        this.name = name;
    }

    public Mono<HttpStatus> getRemoteStatus() {
        return buildRequest(Collections.EMPTY_MAP, statusEndpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(HttpStatus.class)
                .doOnSuccess(res -> moduleService.getById(name)
                            .flatMap(module -> {
                                // Only update status if it's changed.
                                if (module.getStatus().equals(ModuleStatus.DOWN.toString())) {
                                    return moduleService.setStatusToActive(name);
                                }
                                return Mono.just(HttpStatus.OK);
                            }).subscribe()
                )
                .doOnError(this::handleHttpError);
    }

    protected void initEndpoints(HttpEndpoint[] endpoints) {
        for (HttpEndpoint endpoint : endpoints) {
            if (endpoint.getType() == EndpointType.STATUS) {
                statusEndpoint = endpoint;
            }
        }
    };

    protected Mono<String[]> performListOperation(String filePrefix, HttpEndpoint endpoint) {
        return buildRequest(getListEndpointParams(filePrefix), endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String[].class)
                .doOnError(this::handleHttpError);
    }

    protected Mono<?> initPendingFiles(String[] files) {
        return Mono.just(files).flatMap(fileList -> betterFileService.initMany(fileList));
    }

    protected Mono<Object> performNlpOperation(Map<String, String> data, HttpEndpoint endpoint) {
        return buildRequest(data, endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                // Use flatMap so the success handling runs before the next step in the sequence.
                .flatMap(this::handleNlpOperationSuccess)
                .doOnError(this::handleHttpError);
    }
    protected Mono<Object> performPostNlpOperation(Map<String, Object> data, HttpEndpoint endpoint) {
        return buildPostRequest(data, endpoint)
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                // Use flatMap so the success handling runs before the next step in the sequence.
                .flatMap(this::handleNlpOperationSuccess)
                .doOnError(this::handleHttpError);
    }

    protected Disposable handleNlpOperationError(WebClientResponseException err, String[] pendingFiles) {
        return reportErrorInPendingFiles(err.getResponseBodyAsString(), pendingFiles).subscribe();
    }

    protected Mono<RestStatus> updateFilesToReady(BetterFile[] files) {
        // Set status to ready.
        for (BetterFile readyFile : files) {
            readyFile.setStatus(FileStatus.READY);
        }

        return Flux.fromArray(files)
                .flatMap(readyFile -> betterFileService.upsertAndRefresh(readyFile, readyFile.getFilename()))
                .then(Mono.just(RestStatus.OK));
    }

    protected Flux<Object> reportErrorInPendingFiles(String errorMessage, String[] pendingFiles) {
        return Flux.fromArray(pendingFiles)
                .flatMap(pendingFile -> fileShareService.delete(pendingFile)
                        .then(betterFileService.getById(pendingFile))
                        .flatMap(fileToUpdate -> {
                            // Set status of files to error.
                            fileToUpdate.setStatus(FileStatus.ERROR);
                            fileToUpdate.setStatus_message(errorMessage);
                            return betterFileService.upsertAndRefresh(fileToUpdate, fileToUpdate.getFilename());
                        }));
    }

    protected WebClient.RequestHeadersSpec<?> buildRequest(Map<String, String> data, HttpEndpoint endpoint) {
        switch (endpoint.getMethod()) {
            case GET:
                return client.get().uri(uriBuilder -> {
                    uriBuilder.pathSegment(endpoint.getPathSegment());
                    uriBuilder.queryParams(convertMapToHeaders(data));
                    return uriBuilder.build();
                });
            case DELETE:
                return client.delete().uri(uriBuilder -> {
                    uriBuilder.pathSegment(endpoint.getPathSegment());
                    uriBuilder.queryParams(convertMapToHeaders(data));
                    return uriBuilder.build();
                });
            // Default to post request.
            default:
                return client.post()
                        .uri(uriBuilder -> uriBuilder.pathSegment(endpoint.getPathSegment()).build())
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(BodyInserters.fromValue(data));
        }
    }

    protected WebClient.RequestHeadersSpec<?> buildPostRequest(Map<String, Object> data, HttpEndpoint endpoint) {
        if (!endpoint.getMethod().equals(HttpMethod.POST)) {
            throw new UnsupportedOperationException("Can only use a Map<String,Obejct> data with a POST request");
        }
        return client.post()
                .uri(uriBuilder -> uriBuilder.pathSegment(endpoint.getPathSegment()).build())
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(data));
    }

    private HttpHeaders convertMapToHeaders(Map<String, String> data) {
        // Build Http Headers for query params.
        HttpHeaders params = new HttpHeaders();
        try {
            for (Map.Entry<String, String> entry : data.entrySet()) {
                params.add(entry.getKey(), entry.getValue());
            }
        }
        catch (ClassCastException e) {
            // Get the class name of the invalid data type.
            throw new InvalidConfigDataTypeException(e.getMessage().split(" ")[1]);
        }

        return params;
    }

    private WebClient buildNlpWebClient(String name) {
        String url = "http://";
        String host = System.getenv().getOrDefault(name.toUpperCase() + "_HOST", "localhost");

        // Get port from app properties so we don't have to hard-code defaults in the code.
        String port = env.getProperty(name + ".port");

        url += host + ":" + port;

        return WebClient.builder().exchangeStrategies(ExchangeStrategies.builder()
                .codecs(configurer -> configurer
                        .defaultCodecs()
                        // Increase the buffer size to 32MB
                        .maxInMemorySize(32 * 1024 * 1024))
                    .build())
                .baseUrl(url).build();
    }

    private Throwable handleHttpError(Throwable err) {
        if (err instanceof ConnectException || err instanceof UnknownHostException) {
            moduleService.getById(name)
                    .flatMap(module -> {
                        // Only update status if it's changed.
                        if (!module.getStatus().equals(ModuleStatus.DOWN.toString())) {
                            return moduleService.setStatusToDown(name);
                        }

                        return Mono.just(RestStatus.OK);
                    }).subscribe();
        }

        return err;
    }
}
