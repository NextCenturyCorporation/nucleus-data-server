package com.ncc.neon.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.DatasetService;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import okhttp3.Request;
import okhttp3.Response;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("better")
@Slf4j
public class BetterController {
    OkHttpClient client = new OkHttpClient();
    ObjectMapper objectMapper = new ObjectMapper();
    RestHighLevelClient elasticSearchClient;
    WebClient enPreprocessorClient;
    WebClient arPreprocessorClient;
    WebClient bpeClient;


    private DatasetService datasetService;

    BetterController(DatasetService datasetService,
                     @Value("${en.preprocessor.port}") String enPreprocessorPort,
                     @Value("${ar.preprocessor.port}") String arPreprocessorPort,
                     @Value("${bpe.port}") String bpePort) {
        this.datasetService = datasetService;

        String enPreprocessorUrl = "http://" +
                this.getEnv("EN_PREPROCESSOR_HOST", "localhost") +
                ":" + enPreprocessorPort;
        String arPreprocessorUrl = "http://" +
                this.getEnv("AR_PREPROCESSOR_HOST", "localhost") +
                ":" + arPreprocessorPort;
        String bpeHost = "http://" +
                this.getEnv("BPE_HOST", "localhost") +
                ":" + bpePort;
        String elasticHost = getEnv("ELASTIC_HOST", "localhost");

        this.enPreprocessorClient = WebClient.create(enPreprocessorUrl);
        this.arPreprocessorClient = WebClient.create(arPreprocessorUrl);
        this.bpeClient = WebClient.create(bpeHost);
        this.elasticSearchClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticHost, 9200, "http")));
    }

    @GetMapping(path = "willOverwrite")
    public boolean willOverwrite(@RequestParam("file") String file) {
        System.out.println(file);
        return true;
    }

    @PostMapping(path = "upload")
    Mono<RestStatus> upload(@RequestPart("file") Mono<FilePart> filePartMono) {
        // TODO: Return error message if file is not provided.

        return filePartMono
                .flatMap(filePart -> {
                    BetterFile pendingFile = new BetterFile(filePart.filename(), 0);
                    return this.addToElasticSearchFilesIndex(pendingFile)
                            .then(refreshFilesIndex().retry(3))
                            .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                            .then(writeFilePartToShare(filePart));
                })
                .doOnError(onError -> {
                    throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error writing file to share.");
                })
                .flatMap(file -> {
                    BetterFile fileToAdd = new BetterFile(file.getName(), file.length());
                    fileToAdd.setStatus(FileStatus.READY);

                    return this.addToElasticSearchFilesIndex(fileToAdd)
                            .doOnError(onError -> deleteShareFile(file.getName()))
                            .then(refreshFilesIndex().retry(3))
                            .doOnSuccess(status -> datasetService.notify(new DataNotification()));
                });
    }

    @DeleteMapping(path = "file/{id}")
    Mono<ResponseEntity<Object>> delete(@PathVariable("id") String id) {
        return getFileById(id)
                .map(fileToDelete -> deleteShareFile(fileToDelete.getFilename()))
                .then(deleteFileById(id))
                .then(refreshFilesIndex().retry(3))
                .then(Mono.just(ResponseEntity.ok().build()))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    @GetMapping(path = "download")
    ResponseEntity<?> download(@RequestParam("file") String file) {
        ResponseEntity<?> res;
        // TODO: don't allow relative paths.
        Path filePath = getRelativeSharePath().resolve(file);

        try {
            Resource resource = new UrlResource(filePath.toUri());

            if (resource.exists() || resource.isReadable()) {
                res = ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_DISPOSITION,
                                "attachment; filename=\"" + resource.getFilename() + "\"")
                        .body(resource);
            }
            else {
                res = ResponseEntity.notFound().build();
            }
        } catch (MalformedURLException e) {
            res = ResponseEntity.badRequest().build();
        }

        return res;
    }

    @GetMapping(path = "tokenize")
    Mono<RestStatus> tokenize(@RequestParam("file") String file, @RequestParam("language") String language) {
        Mono<BetterFile[]> fileList = Mono.empty();

        if (language.equals("en")) {
            fileList = getEnPreprocessingList(file)
                    .flatMapMany(this::addManyToElasticSearchFilesIndex)
                    .then(refreshFilesIndex().retry(3))
                    .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                    .then(performEnPreprocessing(file));
        } else if (language.equals("ar")) {
            fileList = performArPreprocessing(file);
        }

        return fileList
                .doOnError(onError -> {
                    if (onError instanceof ConnectException || onError instanceof UnknownHostException) {
                        String message = "Failed to connect to " + language.toUpperCase() + " preprocessor.";
                        throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
                    }
                })
                .map(readyFiles -> {
                    for (BetterFile readyFile : readyFiles) {
                        readyFile.setStatus(FileStatus.READY);
                    }
                    return readyFiles;
                })
                .flatMapMany(this::updateManyElasticSearchFile)
                .then(refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    @GetMapping(path = "bpe")
    Mono<RestStatus> bpe(@RequestParam("file") String file) {
        return performBPE(file)
                .flatMap(this::addToElasticSearchFilesIndex)
                .then(refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    @GetMapping(path = "train-mt")
    ResponseEntity<?> train_mt(@RequestParam("tSource") String tSource, @RequestParam("tTarget") String tTarget,
                               @RequestParam("vSource") String vSource, @RequestParam("vTarget") String vTarget) {
        String basename = tSource.substring(0, tSource.length()-7);
        String url = "http://localhost:5001/?train_src=" + tSource + "&train_tgt=" + tTarget + "&valid_src=" + vSource + "&valid_tgt=" + vTarget + "&output_basename=" + basename;
        ResponseEntity<?> responseEntity = executeGETRequest(url);
        return responseEntity;
    }

    private ResponseEntity<?> executeGETRequest(String url) {
        Request req = new Request.Builder()
                .url(url)
                .get()
                .build();
        try {
            Response response = client.newCall(req).execute();
            BetterFile[] bf = objectMapper.readValue(response.body().string(), BetterFile[].class);
            for(BetterFile f : bf) {
                System.out.println(f.getFilename());
            }
            storeInES(bf);
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (IOException ioe) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private Mono<String[]> getEnPreprocessingList(String filename) {
        return enPreprocessorClient.get()
                .uri(uriBuilder -> {
                    uriBuilder.pathSegment("list");
                    uriBuilder.queryParam("file", filename);
                    return uriBuilder.build();
                })
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String[].class);
    }

    private Mono<BetterFile[]> performEnPreprocessing(String filename) {
        return enPreprocessorClient.get()
                .uri(uriBuilder ->
                        uriBuilder.queryParam("file", filename).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(BetterFile[].class);
    }

    private Mono<BetterFile[]> performArPreprocessing(String filename) {
        return arPreprocessorClient.get()
                .uri(uriBuilder ->
                        uriBuilder.queryParam("file", filename).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(BetterFile[].class);
    }

    private Mono<BetterFile> performBPE(String filename) {
        return bpeClient.get()
                .uri(uriBuilder ->
                        uriBuilder.queryParam("file", filename).build())
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(BetterFile.class);
    }

    private Mono<RestStatus> storeInES(BetterFile[] bf) {
        for (BetterFile file: bf) {
            Map<String, Object> bfMapper = objectMapper.convertValue(file, Map.class);
            IndexRequest indexRequest = new IndexRequest("files", "filedata").source(bfMapper);

            // Wrap the async part in a mono.
            return Mono.create(sink -> {
                try {
                    IndexResponse response = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                    RefreshResponse refResponse = elasticSearchClient.indices().refresh(new RefreshRequest("files"), RequestOptions.DEFAULT);
                    sink.success(refResponse.getStatus());
                } catch (IOException e) {
                    sink.error(e);
                }
            });
        }

        return Mono.justOrEmpty(Optional.empty());
    }

    private Flux<Tuple2<String, RestStatus>> addManyToElasticSearchFilesIndex(BetterFile[] filesToAdd) {
        return Flux.fromArray(filesToAdd)
                .flatMap(this::addToElasticSearchFilesIndex);
    }

    private Flux<Tuple2<String, RestStatus>> addManyToElasticSearchFilesIndex(String[] filesToAdd) {
        return Flux.fromArray(filesToAdd)
                .flatMap(fileToAdd -> addToElasticSearchFilesIndex(new BetterFile(fileToAdd, 0)));
    }

    private Mono<Tuple2<String, RestStatus>> addToElasticSearchFilesIndex(BetterFile fileToAdd) {
        // Serialize file to json map.
        Map<String, Object> bfMapper = objectMapper.convertValue(fileToAdd, Map.class);

        // Build the elasticsearch request.
        IndexRequest indexRequest = new IndexRequest("files", "filedata").source(bfMapper).id(fileToAdd.getFilename());

        // Wrap the async part in a mono.
        return Mono.create(sink -> {
            try {
                IndexResponse indexResponse = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                if (indexResponse.status() != RestStatus.CREATED && indexResponse.status() != RestStatus.OK) {
                    deleteShareFile(fileToAdd.getFilename());
                }

                sink.success(Tuples.of(fileToAdd.getFilename(), indexResponse.status()));
            } catch (ConnectException e) {
                deleteShareFile(fileToAdd.getFilename());
                sink.error(new Exception("Could not connect to database."));
            } catch (IOException e) {
                deleteShareFile(fileToAdd.getFilename());
                sink.error(new Exception("Failed to add file to database."));
            }
        });
    }

    private Flux<Tuple2<String, RestStatus>> updateManyElasticSearchFile(BetterFile[] filesToUpdate) {
        return Flux.fromArray(filesToUpdate)
                .flatMap(this::updateElasticSearchFile);
    }

    private Mono<Tuple2<String, RestStatus>> updateElasticSearchFile(BetterFile fileToUpdate) {
        // Serialize file to json map.
        Map<String, Object> bfMapper = objectMapper.convertValue(fileToUpdate, Map.class);

        // Build the elasticsearch request.
        UpdateRequest updateRequest = new UpdateRequest("files", "filedata", fileToUpdate.getFilename()).doc(bfMapper);

        return Mono.create(sink -> {
           try {
               UpdateResponse updateResponse = elasticSearchClient.update(updateRequest, RequestOptions.DEFAULT);
               sink.success(Tuples.of(fileToUpdate.getFilename(), updateResponse.status()));
           } catch (ConnectException e) {
               sink.error(new Exception("Could not connect to database."));
           } catch (IOException e) {
               sink.error(new Exception("Failed to refresh files index."));
           }
        });
    }

    private Mono<RestStatus> refreshFilesIndex() {
        return Mono.create(sink -> {
            try {
                RefreshResponse refreshResponse = elasticSearchClient.indices().refresh(new RefreshRequest("files"), RequestOptions.DEFAULT);
                sink.success(refreshResponse.getStatus());
            } catch (ConnectException e) {
                sink.error(new Exception("Could not connect to database."));
            } catch (IOException e) {
                sink.error(new Exception("Failed to refresh files index."));
            }
        });
    }

    private Mono<BetterFile> getFileById(String id) {
        // Get the file doc by id.
        GetRequest gr = new GetRequest("files", "filedata", id);

        return Mono.create(sink -> {
            try {
                GetResponse response = elasticSearchClient.get(gr, RequestOptions.DEFAULT);

                // Send 404 if file does not exist in database.
                if (response.getSource() == null) {
                    sink.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "File not found."));
                }

                BetterFile res = new ObjectMapper().readValue(response.getSourceAsString(), BetterFile.class);

                sink.success(res);
            } catch (ConnectException e) {
                sink.error(new Exception("Could not connect to database."));
            } catch (IOException e) {
                sink.error(e);
            }
        });
    }

    private Mono<RestStatus> deleteFileById(String id) {
        DeleteRequest dr = new DeleteRequest("files", "filedata", id);

        return Mono.create(sink -> {
            try {
                DeleteResponse response = elasticSearchClient.delete(dr, RequestOptions.DEFAULT);
                sink.success(response.status());
            } catch (ConnectException e) {
                sink.error(new Exception("Could not connect to database."));
            } catch (IOException e) {
                sink.error(new Exception("Failed to delete file from database."));
            }
        });
    }

    private Mono<File> writeFilePartToShare(FilePart filePart) {
        Path sharePath = getRelativeSharePath();
        File dir = new File(sharePath.toString());

        if (!dir.exists()) {
            dir.mkdir();
        }

        Path filePath = sharePath.resolve(Objects.requireNonNull(filePart.filename()));

        return filePart.transferTo(filePath)
                .thenReturn(new File(filePath.toString()));
    }

    private Mono<Boolean> deleteShareFile(String fileToDelete) {
        // Append filename to share directory.
        String filepath = getRelativeSharePath().resolve(fileToDelete).toString();
        return Mono.just(new File(filepath).delete());
    }

    private Path getRelativeSharePath() {
        String shareDir = System.getenv("SHARE_DIR");

        // Default to a known directory.
        if (shareDir == null) {
            shareDir = "share";
        }

        return Paths.get(".").resolve(shareDir);
    }

    private String getEnv(String key, String defaultVal) {
        String val = System.getenv(key);

        if (val == null) {
            val = defaultVal;
        }

        return val;
    }

}
