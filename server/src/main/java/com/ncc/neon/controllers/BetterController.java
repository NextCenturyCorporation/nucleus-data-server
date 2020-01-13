package com.ncc.neon.controllers;

import com.ncc.neon.common.LanguageCode;
import com.ncc.neon.common.NlpClientQueryBuilder;
import com.ncc.neon.common.RemoteNlpClient;
import com.ncc.neon.exception.UpsertException;
import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.nio.file.Path;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("better")
@Slf4j
public class BetterController {
    WebClient enPreprocessorClient;
    WebClient arPreprocessorClient;
    WebClient bpeClient;
    WebClient nmtClient;
    RemoteNlpClient enPreprocessorRNC;
    RemoteNlpClient arPreprocessorRNC;
    RemoteNlpClient bpeRNC;
    RemoteNlpClient nmtRNC;

    private DatasetService datasetService;
    private FileShareService fileShareService;
    private BetterFileService betterFileService;

    BetterController(DatasetService datasetService,
                     FileShareService fileShareService,
                     BetterFileService betterFileService,
                     @Value("${en.preprocessor.port}") String enPreprocessorPort,
                     @Value("${ar.preprocessor.port}") String arPreprocessorPort,
                     @Value("${bpe.port}") String bpePort,
                     @Value("${nmt.port}") String nmtPort) {
        this.datasetService = datasetService;
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;

        String enPreprocessorUrl = "http://" +
                System.getenv().getOrDefault("EN_PREPROCESSOR_HOST", "localhost") +
                ":" + enPreprocessorPort;
        String arPreprocessorUrl = "http://" +
                System.getenv().getOrDefault("AR_PREPROCESSOR_HOST", "localhost") +
                ":" + arPreprocessorPort;
        String bpeHost = "http://" +
                System.getenv().getOrDefault("BPE_HOST", "localhost") +
                ":" + bpePort;
        String nmtHost = "http://" +
                System.getenv().getOrDefault("NMT_HOST", "localhost") +
                ":" + nmtPort;

        this.enPreprocessorClient = WebClient.create(enPreprocessorUrl);
        this.arPreprocessorClient = WebClient.create(arPreprocessorUrl);
        this.bpeClient = WebClient.create(bpeHost);
        this.nmtClient = WebClient.create(nmtHost);
        enPreprocessorRNC = new RemoteNlpClient(enPreprocessorClient, this.datasetService, this.fileShareService, this.betterFileService);
        arPreprocessorRNC = new RemoteNlpClient(arPreprocessorClient, this.datasetService, this.fileShareService, this.betterFileService);
        bpeRNC = new RemoteNlpClient(bpeClient, this.datasetService, this.fileShareService, this.betterFileService);
        nmtRNC = new RemoteNlpClient(nmtClient, this.datasetService, this.fileShareService, this.betterFileService);
    }

    @PostMapping(path = "upload")
    Mono<RestStatus> upload(@RequestPart("file") Mono<FilePart> filePartMono) {
        // TODO: Return error message if file is not provided.

        return filePartMono
                .flatMap(filePart -> {
                    // Create pending file in ES.
                    BetterFile pendingFile = new BetterFile(filePart.filename(), 0);
                    return betterFileService.upsert(pendingFile)
                            .then(betterFileService.refreshFilesIndex().retry(3))
                            .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                            // Write file part to share.
                            .then(fileShareService.writeFilePart(filePart));
                })
                .doOnError(onError -> {
                    if (onError instanceof UpsertException) {
                        Mono.just(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, onError.getMessage()));
                    } else {
                        filePartMono.flatMap(errorFilePart -> betterFileService.getById(errorFilePart.filename())
                                .doOnError(getErr -> new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, getErr.getMessage()))
                                .flatMap(errorFile -> {
                                    errorFile.setStatus(FileStatus.ERROR);
                                    errorFile.setStatus_message(onError.getClass() + ": " + onError.getMessage());
                                    return betterFileService.upsert(errorFile);
                                })
                                .then(betterFileService.refreshFilesIndex().retry(3))
                                .doOnSuccess(status -> datasetService.notify(new DataNotification())))
                                .then(Mono.just(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error writing file to share.")))
                                .subscribe();
                    }
                })
                .flatMap(file -> {
                    // File successfully written.  Set file status to ready.
                    BetterFile fileToAdd = new BetterFile(file.getName(), file.length());
                    fileToAdd.setStatus(FileStatus.READY);

                    // Update entries in ES.
                    return betterFileService.upsert(fileToAdd)
                            // Delete file if update fails.
                            .doOnError(onError -> fileShareService.delete(file.getName()))
                            .then(betterFileService.refreshFilesIndex().retry(3))
                            .doOnSuccess(status -> datasetService.notify(new DataNotification()));
                });
    }

    @DeleteMapping(path = "file/{id}")
    Mono<ResponseEntity<Object>> delete(@PathVariable("id") String id) {
        return betterFileService.getById(id)
                .map(fileToDelete -> {
                    if (fileToDelete == null) {
                        return Mono.just(ResponseEntity.notFound().build());
                    }
                    return fileShareService.delete(fileToDelete.getFilename());
                })
                .then(betterFileService.deleteById(id))
                .then(betterFileService.refreshFilesIndex().retry(3))
                .then(Mono.just(ResponseEntity.ok().build()))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    @GetMapping(path = "download")
    ResponseEntity<?> download(@RequestParam("file") String file) {
        ResponseEntity<?> res;
        // TODO: don't allow relative paths.
        Path filePath = fileShareService.sharePath.resolve(file);

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
    Mono<RestStatus> tokenize(@RequestParam("file") String file, @RequestParam("language") LanguageCode languageCode) {
        HttpHeaders tokenizeParam = NlpClientQueryBuilder.buildFileOperationQuery(file);
        if (languageCode == LanguageCode.AR) {
            return arPreprocessorRNC.performNlpOperation(tokenizeParam, arPreprocessorRNC.getOutputFileList(tokenizeParam));
        } else {
            return enPreprocessorRNC.performNlpOperation(tokenizeParam, enPreprocessorRNC.getOutputFileList(tokenizeParam));
        }
    }

    @GetMapping(path = "bpe")
    Mono<RestStatus> bpe(@RequestParam("file") String file) {
        HttpHeaders bpeParam = NlpClientQueryBuilder.buildFileOperationQuery(file);
        return bpeRNC.performNlpOperation(NlpClientQueryBuilder.buildFileOperationQuery(file), bpeRNC.getOutputFileList(bpeParam));
    }

    @GetMapping(path = "train-mt")
    Mono<RestStatus> train_mt(@RequestParam("basename") String basename, @RequestParam("tSource") String tSource, @RequestParam("tTarget") String tTarget,
                               @RequestParam("vSource") String vSource, @RequestParam("vTarget") String vTarget) {
        HttpHeaders nmtFileParam = NlpClientQueryBuilder.buildFilePrefixQuery(basename);
        HttpHeaders nmtOperationParam = NlpClientQueryBuilder.buildTrainingOperationQuery(basename, tSource, tTarget, vSource, vTarget);
        return nmtRNC.performNlpOperation(nmtOperationParam, nmtRNC.getOutputFileList(nmtFileParam));
    }
}
