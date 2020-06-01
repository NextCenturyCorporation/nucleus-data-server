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

    public Mono<Object> processExperiment(ExperimentConfig experimentConfig, boolean infOnly, boolean runEval) {
        return moduleService.buildNlpModuleClient(experimentConfig.module)
                .flatMap(nlpModule -> experimentService.insertNew(experimentConfig)
                        .flatMap(insertRes -> Flux.fromArray(experimentConfig.getEvalConfigs())
                                .flatMapSequential(config -> experimentService.incrementCurrRun(insertRes)
                                                .flatMap(ignored -> processEvaluation(insertRes, config, (IENlpModule) nlpModule, infOnly, runEval, experimentConfig.getTestFile())
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

    private Mono<String> processEvaluation(String experimentId, EvalConfig config, IENlpModule ieNlpModule1, boolean infOnly, boolean runEval, String testFile) {
        // Get the run id for the entire evaluation.
        String runId;
        try {
            runId = runService.initRunSync(experimentId, config.getTrainConfigParams(), config.getInfConfigParams());
        } catch (UpsertException e) {
            return Mono.error(e);
        }

        // Variables used in flatMaps must be final.
        String finalRunId = runId;

        Mono<Object> trainMono = moduleService.incrementJobCount(ieNlpModule1.getName())
                .flatMap(ignored -> {
                    // Set job id equal to the new run id.
                    config.setJobIdParam(finalRunId);

                    if (!infOnly) {
                        return runService.updateToTrainStatus(finalRunId)
                                .flatMap(test -> ieNlpModule1.performTraining(config, finalRunId))
                                .doOnError(trainError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, trainError.getMessage())));
                    }

                    return Mono.empty();
                });

        Mono<RestStatus> infMono = ieNlpModule1.performInference(config, runId)
                        .doOnError(infError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, infError.getMessage())))
                        .flatMap(infRes -> moduleService.decrementJobCount(ieNlpModule1.getName()));

        Mono<RestStatus> evalMono = moduleService.buildNlpModuleClient(ModuleService.EVAL_SERVICE_NAME)
                        .flatMap(evalModule -> {
                            EvalNlpModule evalNlpModule = (EvalNlpModule) evalModule;

                            if (!runEval) {
                                return runService.getInferenceOutput(runId)
                                        .flatMap(sysFile -> evalNlpModule.performEval(testFile, sysFile, runId)
                                                .doOnError(evalError -> {
                                                    evalNlpModule.handleErrorDuringRun(evalError, runId);
                                                    Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, evalError.getMessage()));
                                                }));
                            }
                            return Mono.empty();
                        });

        return trainMono
                .flatMap(trainRes -> infMono
                        .flatMap(infRes -> evalMono
                            .flatMap(evalRes -> Mono.just(runId))));
    }
}
