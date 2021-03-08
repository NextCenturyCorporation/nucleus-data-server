package com.ncc.neon.services;

import com.ncc.neon.better.*;
import com.ncc.neon.exception.UpsertException;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

@Component
public class AsyncService {

    private ModuleService moduleService;
    private ExperimentService experimentService;
    private RunService runService;

    @Autowired
    private Environment env;

    @Autowired
    public AsyncService(ModuleService moduleService, ExperimentService experimentService, RunService runService) {
        this.moduleService = moduleService;
        this.experimentService = experimentService;
        this.runService = runService;
    }

    public Mono<Object> processExperiment(ExperimentConfig experimentConfig, boolean infOnly) {
        return moduleService.buildNlpModuleClient(experimentConfig.module)
                .flatMap(nlpModule -> experimentService.insertNew(experimentConfig)
                        .flatMap(insertRes -> Flux.fromArray(experimentConfig.getEvalConfigs())
                                .flatMapSequential(config -> experimentService.incrementCurrRun(insertRes)
                                                .flatMap(ignored -> processEvaluation(insertRes, config, (IENlpModule) nlpModule, infOnly, experimentConfig.getTestFile())
                                                        .flatMap(completedRunId -> experimentService.updateOnRunComplete(completedRunId, insertRes)
                                                                .then(experimentService.checkForComplete(insertRes)))
                                                        .onErrorResume(err -> experimentService.countErrorEvals(insertRes)
                                                                .then(experimentService.checkForComplete(insertRes)))),
                                        Integer.parseInt(Objects.requireNonNull(env.getProperty("server_gpu_count")))).collectList()));
    }

    public Mono<?> performPreprocess(String file, String module) {
        return moduleService.buildNlpModuleClient(module).flatMap(nlpModule -> {
            PreprocessorNlpModule preprocessorNlpModule = (PreprocessorNlpModule)nlpModule;

            return moduleService.incrementJobCount(nlpModule.getName())
                    .flatMap(ignored -> preprocessorNlpModule.performPreprocessing(file))
                    .flatMap(ignored -> moduleService.decrementJobCount(nlpModule.getName()));
        });
    }

    private Mono<String> processEvaluation(String experimentId, EvalConfig config, IENlpModule ieNlpModule, boolean infOnly, String testFile) {
        // Get the run id for the entire evaluation.
        String runId;
        try {
            runId = runService.initRunSync(experimentId, config.getTrainConfigParams(), config.getInfConfigParams());
        } catch (UpsertException e) {
            return Mono.error(e);
        }

        // Variables used in flatMaps must be final.
        String finalRunId = runId;

        Mono<Object> trainMono = moduleService.incrementJobCount(ieNlpModule.getName())
                .flatMap(ignored -> {
                    // Set job id equal to the new run id.
                    config.setJobIdParam(finalRunId);

                    if (!infOnly) {
                        return runService.updateToTrainStatus(finalRunId)
                                .flatMap(test -> ieNlpModule.performTraining(config, finalRunId))
                                .doOnError(trainError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, trainError.getMessage())));
                    }

                    return Mono.empty();
                });

        Mono<RestStatus> infMono = ieNlpModule.performInference(config, runId)
                        .doOnError(infError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, infError.getMessage())))
                        .flatMap(infRes -> moduleService.decrementJobCount(ieNlpModule.getName()));

        Mono<RestStatus> evalMono = moduleService.buildNlpModuleClient(ModuleService.EVAL_SERVICE_NAME)
                        .flatMap(evalModule -> {
                            EvalNlpModule evalNlpModule = (EvalNlpModule) evalModule;
                            return runService.getInferenceOutput(runId)
                                    .flatMap(sysFile -> evalNlpModule.performEval(testFile, sysFile, runId)
                                            .doOnError(evalError -> {
                                                evalNlpModule.handleErrorDuringRun(evalError, runId);
                                                Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, evalError.getMessage()));
                                            }));
                        });

        return trainMono
                .flatMap(trainRes -> infMono
                        .flatMap(infRes -> evalMono
                            .flatMap(evalRes -> Mono.just(runId))));
    }

//    private Mono<String> processIREvaluation(String experimentId, EvalConfig config, IENlpModule ieNlpModule) {
//        // Get the run id for the entire evaluation.
//        String runId;
//        try {
//            runId = runService.initRunSync(experimentId, config.getTrainConfigParams(), config.getInfConfigParams());
//        } catch (UpsertException e) {
//            return Mono.error(e);
//        }
//
//        // Variables used in flatMaps must be final.
//        String finalRunId = runId;
//
//        Mono<Object> trainMono = moduleService.incrementJobCount(ieNlpModule.getName())
//                .flatMap(ignored -> {
//                    // Set job id equal to the new run id.
//                    config.setJobIdParam(finalRunId);
//
//                    if (!infOnly) {
//                        return runService.updateToTrainStatus(finalRunId)
//                                .flatMap(test -> ieNlpModule.performTraining(config, finalRunId))
//                                .doOnError(trainError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, trainError.getMessage())));
//                    }
//
//                    return Mono.empty();
//                });
//
//        Mono<RestStatus> infMono = ieNlpModule.performInference(config, runId)
//                .doOnError(infError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, infError.getMessage())))
//                .flatMap(infRes -> moduleService.decrementJobCount(ieNlpModule.getName()));
//
//        Mono<RestStatus> evalMono = moduleService.buildNlpModuleClient(ModuleService.EVAL_SERVICE_NAME)
//                .flatMap(evalModule -> {
//                    EvalNlpModule evalNlpModule = (EvalNlpModule) evalModule;
//                    return runService.getInferenceOutput(runId)
//                            .flatMap(sysFile -> evalNlpModule.performEval(testFile, sysFile, runId)
//                                    .doOnError(evalError -> {
//                                        evalNlpModule.handleErrorDuringRun(evalError, runId);
//                                        Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, evalError.getMessage()));
//                                    }));
//                });
//
//        return trainMono
//                .flatMap(trainRes -> infMono
//                        .flatMap(infRes -> evalMono
//                                .flatMap(evalRes -> Mono.just(runId))));
//    }
}
