package com.ncc.neon.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.ncc.neon.models.BetterFile;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okhttp3.RequestBody;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("better")
@Slf4j
public class BetterController {
    OkHttpClient client = new OkHttpClient();
    ObjectMapper objectMapper = new ObjectMapper();
    RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200, "http"),
            new HttpHost("localhost", 9205, "http")
    ).build();

    @GetMapping(path = "willOverwrite")
    public boolean willOverwrite(@RequestParam("file") String file) {
        System.out.println(file);
        return true;
    }

    @PostMapping(path = "upload")
    String upload(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return "file is empty";
        }
        try {

            // Get the file and save it somewhere
            byte[] bytes = file.getBytes();
            Path path = Paths.get("/Users/bpham/test/" + file.getOriginalFilename());
            Files.write(path, bytes);

        } catch (IOException e) {
            e.printStackTrace();
            return "failed";
        }

        return "success";
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
            BetterFile bf = objectMapper.readValue(response.body().string(), BetterFile.class);
            storeInES(bf);
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (IOException ioe) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void storeInES(BetterFile bf) {

    }

}
