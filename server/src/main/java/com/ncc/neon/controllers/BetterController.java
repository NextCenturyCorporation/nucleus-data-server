package com.ncc.neon.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
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
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
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
    WebClient bpeClient = WebClient.create("http://bpe:5000");


    private DatasetService datasetService;

    @Autowired
    private Environment env;

    BetterController(DatasetService datasetService, @Value("${en.preprocessor.port}") String enPreprocessorPort, @Value("${ar.preprocessor.port}") String arPreprocessorPort) {
        this.datasetService = datasetService;

        String enPreprocessorUrl = "http://" +
                this.getEnv("EN_PREPROCESSOR_HOST", "localhost") +
                ":" +
                this.getEnv("EN_PREPROCESSOR_PORT", enPreprocessorPort);
        String arPreprocessorUrl = "http://" +
                this.getEnv("AR_PREPROCESSOR_HOST", "localhost") +
                ":" +
                this.getEnv("AR_PREPROCESSOR_PORT", arPreprocessorPort);
        String elasticHost = getEnv("ELASTIC_HOST", "localhost");

        this.enPreprocessorClient = WebClient.create(enPreprocessorUrl);
        this.arPreprocessorClient = WebClient.create(arPreprocessorUrl);
        this.elasticSearchClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticHost, 9200, "http")));
    }

    @GetMapping(path = "willOverwrite")
    public boolean willOverwrite(@RequestParam("file") String file) {
        System.out.println(file);
        return true;
    }

    @PostMapping(path = "upload")
    ResponseEntity<Mono<?>> upload(@RequestPart("file") Mono<FilePart> file) {
        // TODO: Return error message if file is not provided.

        Mono<?> uploadFileMono = writeFileToShare(file)
                .flatMap(fileRef -> addToElasticSearchFilesIndex(new BetterFile(fileRef.getName(), fileRef.length())))
                // TODO: Add a doOnError.
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));

        return ResponseEntity.ok().body(uploadFileMono);
    }

    @DeleteMapping(path = "file/{id}")
    ResponseEntity<Mono<?>> delete(@PathVariable("id") String id) {
        Mono<?> deleteFileMono = getFileById(id)
                .flatMap(betterFile -> deleteShareFile(betterFile.getFilename()))
                .then(deleteFileById(id))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));

        return ResponseEntity.ok().body(deleteFileMono);
    }

    @GetMapping(path = "download")
    @ResponseBody
    ResponseEntity<?> download(@RequestParam("file") String file) {
        ResponseEntity res;
        // TODO: don't allow relative paths.
        String shareDir = System.getenv("SHARE_DIR");

        // Default to a known directory.
        if (shareDir == null) {
            shareDir = "share";
        }

        Path fileRef = Paths.get(shareDir).resolve(file);
        Resource resource = null;

        try {
            resource = new UrlResource(fileRef.toUri());

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
            e.printStackTrace();
            res = ResponseEntity.badRequest().build();
        }

        return res;
    }

    @GetMapping(path = "tokenize")
    Flux<Tuple2<String, RestStatus>> tokenize(@RequestParam("file") String file, @RequestParam("language") String language) {
        Mono<BetterFile[]> fileList = Mono.empty();

        if (language.equals("en")) {
            fileList = performEnPreprocessing(file);
        } else if (language.equals("ar")) {
            fileList = performArPreprocessing(file);
        }

        return fileList.flatMapMany(this::addManyToElasticSearchFilesIndex)
                .doOnComplete(() -> datasetService.notify(new DataNotification()));
    }

    @GetMapping(path = "bpe")
    ResponseEntity<?> bpe(@RequestParam("file") String file) {
        Mono<?> bpeMono = performBPE(file)
                .flatMap(this::addToElasticSearchFilesIndex)
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
        return ResponseEntity.ok().body(bpeMono);
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

    private Mono<Tuple2<String, RestStatus>> addToElasticSearchFilesIndex(BetterFile fileToAdd) {
        // Serialize file to json map.
        Map<String, Object> bfMapper = objectMapper.convertValue(fileToAdd, Map.class);

        // Build the elasticsearch request.
        IndexRequest indexRequest = new IndexRequest("files", "filedata").source(bfMapper);

        // Wrap the async part in a mono.
        return Mono.create(sink -> {
            try {
                IndexResponse response = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                RefreshResponse refResponse = elasticSearchClient.indices().refresh(new RefreshRequest("files"), RequestOptions.DEFAULT);
                sink.success(Tuples.of(fileToAdd.getFilename(), refResponse.getStatus()));
            } catch (IOException e) {
                sink.error(e);
            }
        });
    }

    private Mono<BetterFile> getFileById(String id) {
        // Get the file doc by id.
        GetRequest gr = new GetRequest("files", "filedata", id);

        return Mono.create(sink -> {
            try {
                GetResponse response = elasticSearchClient.get(gr, RequestOptions.DEFAULT);
                BetterFile res = new ObjectMapper().readValue(response.getSourceAsString(), BetterFile.class);
                sink.success(res);
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
                RefreshResponse refResponse = elasticSearchClient.indices().refresh(new RefreshRequest("files"), RequestOptions.DEFAULT);
                sink.success(refResponse.getStatus());
            } catch (IOException e) {
                sink.error(e);
            }
        });
    }

    private Mono<File> writeFileToShare(Mono<FilePart> fileMono) {
        Path sharePath = getRelativeSharePath();

        return fileMono.flatMap(file -> {
            Path filePath = sharePath.resolve(Objects.requireNonNull(file.filename()));
            file.transferTo(filePath);
            return Mono.just(new File(filePath.toString()));
        });
    }

    private Mono<Boolean> deleteShareFile(String filename) {
        // Append filename to share directory.
        String filepath = getRelativeSharePath().resolve(filename).toString();

        return Mono.create(sink -> {
            sink.success(new File(filepath).delete());
        });
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
