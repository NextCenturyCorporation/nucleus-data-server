package com.ncc.neon.controllers;

import com.ncc.neon.common.BetterFileMachineTrainer;
import com.ncc.neon.common.BetterFileOperationHandler;
import com.ncc.neon.common.LanguageCode;
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
import reactor.core.publisher.Flux;
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
                    filePartMono.flatMap(errorFilePart -> betterFileService.getById(errorFilePart.filename())
                        .flatMap(errorFile -> {
                            errorFile.setStatus(FileStatus.ERROR);
                            errorFile.setStatus_message(onError.getClass() + ": " + onError.getMessage());
                            return betterFileService.upsert(errorFile);
                        })
                        .then(betterFileService.refreshFilesIndex().retry(3))
                        .doOnSuccess(status -> datasetService.notify(new DataNotification())))
                        .then(Mono.just(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error writing file to share.")))
                        .subscribe();
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
                .map(fileToDelete -> fileShareService.delete(fileToDelete.getFilename()))
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
        BetterFileOperationHandler tokenizer = null;

        if (languageCode == LanguageCode.AR) {
            tokenizer = new BetterFileOperationHandler(arPreprocessorClient);
        } else {
            tokenizer = new BetterFileOperationHandler(enPreprocessorClient);
        }

        BetterFileOperationHandler finalTokenizer = tokenizer;
        return tokenizer.getOutputFileList(file)
                // Add pending files.
                .flatMapMany(fileList -> betterFileService.initMany(fileList)
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                // Perform tokenization.
                .then(finalTokenizer.performFileOperation(file))
                .doOnError(onError -> {
                    Flux.fromArray(fileList)
                            // Remove any generated files from share.
                            .flatMap(filename -> fileShareService.delete(filename)
                            .then(betterFileService.getById(filename))
                            .flatMap(fileToUpdate -> {
                                // Set status of files to error.
                                fileToUpdate.setStatus(FileStatus.ERROR);
                                fileToUpdate.setStatus_message(onError.getMessage());
                                return betterFileService.upsert(fileToUpdate);
                            }))
                    .then(betterFileService.refreshFilesIndex().retry(3))
                    .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                    .subscribe();
                }))
                .flatMap(readyFiles -> {
                    // Set status to ready.
                    for (BetterFile readyFile : readyFiles) {
                        readyFile.setStatus(FileStatus.READY);
                    }

                    return betterFileService.upsertMany(readyFiles);
                })
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    @GetMapping(path = "bpe")
    Mono<RestStatus> bpe(@RequestParam("file") String file) {
        final BetterFileOperationHandler encoder = new BetterFileOperationHandler(bpeClient);

        return encoder.getOutputFileList(file)
                .flatMapMany(fileList -> betterFileService.initMany(fileList)
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                .then(encoder.performFileOperation(file))
                .doOnError(onError -> {
                    Flux.fromArray(fileList)
                            .flatMap(filename -> fileShareService.delete(filename)
                            .then(betterFileService.getById(filename))
                            .flatMap(fileToUpdate -> {
                                // Set status of files to error.
                                fileToUpdate.setStatus(FileStatus.ERROR);
                                fileToUpdate.setStatus_message(onError.getMessage());
                                return betterFileService.upsert(fileToUpdate);
                            }))
                            .then(betterFileService.refreshFilesIndex().retry(3))
                            .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                            .subscribe();
                }))
                .flatMap(readyFiles -> {
                    // Set status to ready.
                    for (BetterFile readyFile : readyFiles) {
                        readyFile.setStatus(FileStatus.READY);
                    }

                    return betterFileService.upsertMany(readyFiles);
                })
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }

    @GetMapping(path = "train-mt")
    Mono<RestStatus> train_mt(@RequestParam("tSource") String tSource, @RequestParam("tTarget") String tTarget,
                               @RequestParam("vSource") String vSource, @RequestParam("vTarget") String vTarget) {
        String basename = tSource.substring(0, tSource.length()-7);
        final BetterFileMachineTrainer trainer = new BetterFileMachineTrainer(nmtClient);

        return trainer.getOutputFileList(basename)
                .flatMapMany(fileList -> betterFileService.initMany(fileList)
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                .then(trainer.performTrainingPreprocessing(tSource, tTarget, vSource, vTarget, basename))
                .doOnError(onError -> {
                    Flux.fromArray(fileList)
                            .flatMap(filename -> fileShareService.delete(filename)
                                    .then(betterFileService.getById(filename))
                                    .flatMap(fileToUpdate -> {
                                        // Set status of files to error.
                                        fileToUpdate.setStatus(FileStatus.ERROR);
                                        fileToUpdate.setStatus_message(onError.getMessage());
                                        return betterFileService.upsert(fileToUpdate);
                                    }))
                            .then(betterFileService.refreshFilesIndex().retry(3))
                            .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                            .subscribe();
                }))
                .flatMap(readyFiles -> {
                    // Set status to ready.
                    for (BetterFile readyFile : readyFiles) {
                        readyFile.setStatus(FileStatus.READY);
                    }

                    return betterFileService.upsertMany(readyFiles);
                })
                .then(betterFileService.refreshFilesIndex().retry(3))
                .doOnSuccess(status -> datasetService.notify(new DataNotification()));
    }
}
