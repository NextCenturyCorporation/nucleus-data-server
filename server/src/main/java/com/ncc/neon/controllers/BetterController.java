package com.ncc.neon.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.BetterFile;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.rest.RestStatus;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import okhttp3.Request;
import okhttp3.Response;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.ResponseBody;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("better")
@Slf4j
public class BetterController {
    OkHttpClient client = new OkHttpClient();
    ObjectMapper objectMapper = new ObjectMapper();
    RestHighLevelClient rhlc = new RestHighLevelClient(RestClient.builder(new HttpHost("elasticsearch", 9200, "http")));

    @GetMapping(path = "willOverwrite")
    public boolean willOverwrite(@RequestParam("file") String file) {
        System.out.println(file);
        return true;
    }

    @PostMapping(path = "upload")
    ResponseEntity<Flux<?>> upload(@RequestPart("file") Mono<FilePart> file) {
        // TODO: Return error message if file is not provided.

        String shareDir = System.getenv("SHARE_DIR");

        // Default to a known directory.
        if (shareDir == null) {
            shareDir = "share";
        }

        final String finalShareDir = Paths.get(".").resolve(shareDir).toString();
        File temp = new File("temp");

        Mono<Void> writeFileMono = file.flatMap(it -> it.transferTo(Paths.get(finalShareDir).resolve(Objects.requireNonNull(it.filename()))));
        Mono<Void> fileRefMono = file.flatMap(it -> it.transferTo(temp));
        Mono<Object> storeInEsMono = file.flatMap(it -> storeInES(new BetterFile[] {new BetterFile(Math.toIntExact(temp.length()), it.filename())}));

        return ResponseEntity.ok().body(Flux.merge(writeFileMono, fileRefMono, storeInEsMono));
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
    ResponseEntity<?> tokenize(@RequestParam("file") String file, @RequestParam("language") String language) throws IOException{
        String url;
        if (language.equals("EN")) {
            url = "http://localhost:5000?file=" + file;
        } else {
            url = "http://localhost:5003?file=" + file;
        }
        ResponseEntity<?> responseEntity = executeGETRequest(url);
        return responseEntity;
    }

    @GetMapping(path = "bpe")
    ResponseEntity<?> bpe(@RequestParam("file") String file) {
        String url = "http://localhost:5004?file=" + file;
        ResponseEntity<?> responseEntity = executeGETRequest(url);
        return responseEntity;
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

    private Mono<RestStatus> storeInES(BetterFile[] bf) {
        for (BetterFile file: bf) {
            UUID uuid = UUID.randomUUID();
            file.setId(uuid.toString());
            Map<String, Object> bfMapper = objectMapper.convertValue(file, Map.class);
            IndexRequest indexRequest = new IndexRequest("files", "filedata", file.getId()).source(bfMapper);
            return Mono.create(sink -> {
                try {
                    IndexResponse response = rhlc.index(indexRequest, RequestOptions.DEFAULT);
                    sink.success(response.status());
                } catch (IOException e) {
                    sink.error(e);
                }
            });
        }

        return Mono.justOrEmpty(Optional.empty());
    }

}
