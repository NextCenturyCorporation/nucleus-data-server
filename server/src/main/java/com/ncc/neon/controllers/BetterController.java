package com.ncc.neon.controllers;

import com.ncc.neon.better.ExperimentConfig;
import com.ncc.neon.better.IENlpModule;
import com.ncc.neon.better.IRNlpModule;
import com.ncc.neon.exception.UpsertException;
import com.ncc.neon.models.ExperimentForm;
import com.ncc.neon.models.FileStatus;
import com.ncc.neon.services.*;
import lombok.extern.slf4j.Slf4j;

import org.json.JSONObject;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("better")
@Slf4j
public class BetterController {

    public static final String SHARE_DIR = System.getenv().getOrDefault("SHARE_DIR", "share");
    public static final Path SHARE_PATH = Paths.get(".").resolve(SHARE_DIR);

    private FileShareService fileShareService;
    private BetterFileService betterFileService;
    private ModuleService moduleService;
    private AsyncService asyncService;
    private IRDataService irDataService;

    BetterController(FileShareService fileShareService,
                     BetterFileService betterFileService,
                     ModuleService moduleService,
                     AsyncService asyncService,
                     IRDataService irDataService) {
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
        this.moduleService = moduleService;
        this.asyncService = asyncService;
        this.irDataService = irDataService;
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
                    return betterFileService.initFile(filePart.filename())
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
                                    return betterFileService.insertAndRefresh(errorFile);
                                }))
                                .then(Mono.just(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error writing file to share.")))
                                .subscribe();
                    }
                })
                .flatMap(file -> {
                    // File successfully written.  Set file status to ready.
                    Map<String, Object> data = new HashMap<>();
                    data.put(BetterFileService.STATUS_FIELD, FileStatus.READY);

                    // Update entries in ES.
                    return betterFileService.updateAndRefresh(data, file.getName())
                            // Delete file if update fails.
                            .doOnError(onError -> fileShareService.delete(file.getName()))
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
                .then(betterFileService.deleteByIdAndRefresh(id))
                .then(Mono.just(ResponseEntity.ok().build()));
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

    @GetMapping(path="irsearch")
    Mono<String[]> irsearch(@RequestParam("query") String query, @RequestParam("module") String module)  {
        // synchronous service 
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule; // static cast from nlpModule -> IR 
                    Mono<String> docfileM = irModule.getDocfile();
                    Mono<String[]> queryResultsM = irModule.searchIR(query);
                    return docfileM.zipWith(queryResultsM, (docfile, queryResults) -> {
                        // load docfile
                        Path filePath = fileShareService.getSharePath().resolve(docfile);
                    	JSONObject documents = loadDocfileFromSharedir(filePath);
                    	for (String docID : queryResults) {
                    		String doc = ""; // introspect on documents to get the one corresponding to docID
                    		String uuid = ""; // introspect on documents to get the one corresponding to docID
                    		this.irDataService.initFile(docID, doc, uuid);
                    	}
                    	return queryResults; // return Mono String[]
                    });
                });               
    }

    @GetMapping(path="preprocess")
    ResponseEntity<Object> preprocess(@RequestParam("file") String file, @RequestParam("module") String module) {
        asyncService.performPreprocess(file, module)
                .subscribeOn(Schedulers.newSingle("thread"))
                .subscribe();

        return ResponseEntity.ok().build();
    }

    @PostMapping(path="experiment")
    ResponseEntity<Object> experiment(@RequestBody ExperimentForm experimentForm) {
        try {
            ExperimentConfig experimentConfig = new ExperimentConfig(experimentForm);

            // Build the experiment config for the evaluation
            asyncService.processExperiment(experimentConfig, experimentForm.isInfOnly())
                .subscribeOn(Schedulers.newSingle("thread"))
                .subscribe();
        } catch (IOException e) {
            return ResponseEntity.badRequest().build();
        }

        return ResponseEntity.ok().build();
    }

    @PostMapping(path="syncexperiment")
    Mono<Object> syncExperiment(@RequestBody ExperimentForm experimentForm) {
        ExperimentConfig experimentConfig;
        try {
            experimentConfig = new ExperimentConfig(experimentForm);
        } catch (IOException e) {
            return Mono.error(e);
        }
        return asyncService.processExperiment(experimentConfig, experimentForm.isInfOnly());
    }

    @DeleteMapping(path="eval/cancel")
    Mono<Object> cancelEval(@RequestParam("module") String module, @RequestParam("run_id") String runId) {
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IENlpModule ieNlpModule = (IENlpModule) nlpModule;
                    return ieNlpModule.cancelEval(runId);
                });
    }
}
