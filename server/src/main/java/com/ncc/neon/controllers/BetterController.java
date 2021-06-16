package com.ncc.neon.controllers;

import com.ncc.neon.better.ExperimentConfig;
import com.ncc.neon.better.IENlpModule;
import com.ncc.neon.better.IRNlpModule;
import com.ncc.neon.exception.UpsertException;
import com.ncc.neon.models.*;
import com.ncc.neon.services.*;
import lombok.extern.slf4j.Slf4j;
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
import java.util.List;
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
    private IRDocEntityService irDocEntityService;

    BetterController(FileShareService fileShareService,
                     BetterFileService betterFileService,
                     ModuleService moduleService,
                     AsyncService asyncService,
                     IRDataService irDataService,
                     IRDocEntityService irDocEntityService) {
        this.fileShareService = fileShareService;
        this.betterFileService = betterFileService;
        this.moduleService = moduleService;
        this.asyncService = asyncService;
        this.irDataService = irDataService;
        this.irDocEntityService = irDocEntityService;
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
            } else {
                res = ResponseEntity.notFound().build();
            }
        } catch (MalformedURLException e) {
            res = ResponseEntity.badRequest().build();
        }

        return res;
    }

    @PostMapping(path="irsearch")
    Mono<Object> irsearch(@RequestBody Map<String, String> body, @RequestParam("module") String module) {
        // synchronous service
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule;
                    // Use the method to send the data via POST
                    return irModule.searchIRPost(body.get("query"));
                });
    }

    /**
     * Pass a request object through to the IR wrapper
     * @param body the request, wrapped in an outer object {request: (Request obj)}
     *             Note that this is a Map - this is generic so that it can be passed through without
     *             having to create a bunch of data classes
     * @param module the module, should be 'ir-wrapper'
     * @return the query result
     */
    @PostMapping(path="irrequest")
    Mono<Object> irRequest(@RequestBody Map<String, Object> body, @RequestParam("module") String module) {
        // synchronous service
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule;
                    // Use the method to send the data via POST
                    return irModule.runIrRequest(body);
                });
    }

    /**
     * Pass a request object through to the IR wrapper for HITL UI
     * @param body the request, wrapped in an outer object {request: (Request obj)}
     *             This is currently empty but fields may be added in the future
     * @param module the module, should be 'ir-wrapper'
     * @return the query result
     */
    @PostMapping(path="hitlrequest")
    Mono<Object> hitlRequest(@RequestBody Map<String, Object> body, @RequestParam("module") String module) {
        // synchronous service
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule;
                    // Use the method to send the data via POST
                    return irModule.runHITLRequest(body);
                });
    }

    /**
     * Pass a request object through to the IR wrapper
     * @param body the request, wrapped in an outer object {request: (relevantJudgementList obj)}
     *             The type is currently a generic map but it may be changed to RelevanceJudgementList object
     * @param module the module, should be 'ir-wrapper'
     * @return the query result
     */
    @PostMapping(path="hitl")
    Mono<Object> hitl(@RequestBody Map<String, Object> body, @RequestParam("module") String module) {
        // synchronous service
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule;
                    // Use the method to send the data via POST
                    return irModule.runHITLRequest(body);
                });
    }

    @GetMapping(path="irsearch")
    Mono<Object> irsearch(@RequestParam("query") String query, @RequestParam("module") String module)  {
        // synchronous service
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule;
                    return irModule.searchIR(query);
                });
        // static cast from nlpModule -> IR
        // return Mono String[]
    }

    /**
     * Given a string query, it calls the ir-wrapper and convert the ranked docid into bp json format
     * @param query query object
     * @param module the module, should be 'ir-wrapper'
     * @return the list of ranked ids for
     */
    @GetMapping(path="irie")
    Mono<Object> irie(@RequestParam("query") String query, @RequestParam("module") String module)  {
        // synchronous service
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule;
                    return irModule.searchIR(query);
                });
        // static cast from nlpModule -> IR
        // return Mono String[]
    }

    /**
     * Return a list of ir doc ids
     * @param docIds the ir doc ids
     * @return doc info for the corresponding doc ids
     */
    @GetMapping(path = "getirdocs")
    List<Docfile> getirdocs(@RequestParam("docIds") String[] ids) throws IOException {
        return this.irDataService.getIRDocResponse("irdata", "irdata", ids);
    }

    /**
     * Returns the entities for a given docid
     * @param docId
     * @return a map of the ir entities
     */
    @GetMapping(path = "irentities")
    Map irDocEntities(@RequestParam("docId") String docId) throws IOException {
        return this.irDocEntityService.getByIdSync(docId);
    }

    @GetMapping(path = "preprocess")
    ResponseEntity<Object> preprocess(@RequestParam("file") String file, @RequestParam("module") String module) {
        asyncService.performPreprocess(file, module)
                .subscribeOn(Schedulers.newSingle("thread"))
                .subscribe();

        return ResponseEntity.ok().build();
    }

    @PostMapping(path = "experiment")
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

    @PostMapping(path = "syncexperiment")
    Mono<Object> syncExperiment(@RequestBody ExperimentForm experimentForm) {
        ExperimentConfig experimentConfig;
        try {
            experimentConfig = new ExperimentConfig(experimentForm);
        } catch (IOException e) {
            return Mono.error(e);
        }
        return asyncService.processExperiment(experimentConfig, experimentForm.isInfOnly());
    }

//    @PostMapping(path = "irexperiment")
//    ResponseEntity<Object> irexperiment(@RequestBody IRExperimentForm experimentForm) {
//
//        String module = "ir_wrapper";
//        return moduleService.buildNlpModuleClient(module)
//                .flatMap(nlpModule -> {
//                    IRNlpModule irModule = (IRNlpModule) nlpModule;
//                    return irModule.start(experimentForm);
//                });
//        return ResponseEntity.ok().build();
//    }

    @DeleteMapping(path = "eval/cancel")
    Mono<Object> cancelEval(@RequestParam("module") String module, @RequestParam("run_id") String runId) {
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IENlpModule ieNlpModule = (IENlpModule) nlpModule;
                    return ieNlpModule.cancelEval(runId);
                });
    }

    /**
     * Deprecated. 2020 attempt to hook HITL UI with IR
     * Pass a list of Relevance Judgement object through to the IR wrapper
     * @param RelevanceJudgementList the relevance Judgement List object which is a list of docids and boolean}
     * @param module the module, should be 'ir-wrapper'
     * @return the IR Response object
     */
    @PostMapping(path = "retrofitter" )
    Mono<IRResponse> retroactive(@RequestBody RelevanceJudgementList rels) throws IOException {
        String module = "ir_wrapper";
        return moduleService.buildNlpModuleClient(module)
                .flatMap(nlpModule -> {
                    IRNlpModule irModule = (IRNlpModule) nlpModule;
                    return irModule.retrofit(rels.getRelList());
                });
    }
}
