package com.ncc.neon.controllers;

import com.ncc.neon.exception.UpsertException;
import com.ncc.neon.models.BetterFile;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.*;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.rest.RestStatus;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.List;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("better")
@Slf4j
public class BetterController {

    private DatasetService datasetService;
    private FileShareService fileShareService;
    private BetterFileService betterFileService;
    private ModuleService moduleService;
    private AsyncService asyncService;

    BetterController(DatasetService datasetService,
                     FileShareService fileShareService,
                     BetterFileService betterFileService,
                     ModuleService moduleService,
                     AsyncService asyncService) {
        this.datasetService = datasetService;
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
        this.moduleService = moduleService;
        this.asyncService = asyncService;
    }

    @PostConstruct
    public void init() {
        moduleService.checkAllConnections().subscribe();
    }

    @GetMapping(path = "status")
    Mono<HttpStatus> status() {
        return moduleService.checkAllConnections();
    }

    @PostMapping(path = "upload")
    Mono<ResponseEntity<?>> upload(@RequestPart("file") Mono<FilePart> filePartMono) {
        // TODO: Return error message if file is not provided.

        return filePartMono
                .flatMap(filePart -> {
                    // Create pending file in ES.
                    BetterFile pendingFile = new BetterFile(filePart.filename(), 0);
                    return betterFileService.upsert(pendingFile)
                            .onErrorResume(Mono::error)
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
                            .doOnSuccess(status -> datasetService.notify(new DataNotification()))
                            .then(Mono.just(ResponseEntity.ok().build()));
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
        Path filePath = fileShareService.getSharePath().resolve(file);
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

    @GetMapping(path="preprocess")
    ResponseEntity<Object> preprocess(@RequestParam("file") String file, @RequestParam("module") String module) {
        asyncService.performPreprocess(file, module)
                .subscribeOn(Schedulers.newSingle("thread"))
                .subscribe();

        return ResponseEntity.ok().build();
    }

    @GetMapping(path="train")
    ResponseEntity<Object> train(@RequestParam("configFile") String configFile, @RequestParam("module") String module, @RequestParam("runId") String runId) {
        asyncService.processTraining(configFile, module, runId)
                .subscribeOn(Schedulers.newSingle("thread"))
                .subscribe();

        return ResponseEntity.ok().build();
    }

    @GetMapping(path="eval")
    ResponseEntity<Object> eval(@RequestParam("trainConfigFile") String trainConfigFile,
                          @RequestParam("infConfigFile") String infConfigFile, @RequestParam("module") String module,
                          @RequestParam("infOnly") boolean infOnly, @RequestParam("refFile") String refFile) {
        asyncService.processEvaluation(trainConfigFile, infConfigFile, module, infOnly, refFile)
                .subscribeOn(Schedulers.newSingle("thread"))
                .subscribe();

        return ResponseEntity.ok().build();
    }
}
