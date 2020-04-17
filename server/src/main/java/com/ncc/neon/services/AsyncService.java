package com.ncc.neon.services;

import com.ncc.neon.better.*;
import com.ncc.neon.models.Run;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

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

    private Mono<String> processEvaluation(String experimentId, EvalConfig config, IENlpModule ieNlpModule1, boolean infOnly, String testFile) {
        String runId = runService.initRun(experimentId, config.getTrainConfigParams(), config.getInfConfigParams()).block();

        Mono<Object> trainMono = moduleService.incrementJobCount(ieNlpModule1.getName())
                .flatMap(ignored -> {
                    // Set job id equal to the new run id.
                    config.setJobIdParam(runId);

                    if (!infOnly) {
                        return runService.updateToTrainStatus(runId)
                                .flatMap(test -> ieNlpModule1.performTraining(config, runId))
                                .doOnError(trainError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, trainError.getMessage())));
                    }

                    return Mono.empty();
                });

        Mono<RestStatus> infMono = runService.updateToInferenceStatus(runId)
                .flatMap(updateRes -> ieNlpModule1.performInference(config, runId)
                        .doOnError(infError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, infError.getMessage())))
                        .flatMap(infRes -> moduleService.decrementJobCount(ieNlpModule1.getName())));

        Mono<RestStatus> evalMono = runService.updateToScoringStatus(runId)
                .flatMap(updatedRun -> moduleService.buildNlpModuleClient(ModuleService.EVAL_SERVICE_NAME)
                        .flatMap(evalModule -> {
                            EvalNlpModule evalNlpModule = (EvalNlpModule) evalModule;
                            return runService.getInferenceOutput(runId)
                                    .flatMap(sysFile -> evalNlpModule.performEval(testFile, sysFile, runId)
                                            .doOnError(evalError -> {
                                                evalNlpModule.handleErrorDuringRun(evalError, runId);
                                                Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, evalError.getMessage()));
                                            }));
                        }));

        return trainMono.flatMap(ignored -> )

        return moduleService.incrementJobCount(ieNlpModule1.getName())
                .flatMap(ignored -> runService.initRun(experimentId, config.getTrainConfigParams(), config.getInfConfigParams())
                    .flatMap(initialRun -> {
                        // Set job id equal to the new run id.
                        config.setJobIdParam(initialRun.getT1());
                        IENlpModule ieNlpModule = (IENlpModule) ieNlpModule1;

                        Mono<Tuple2<String, IENlpModule>> res = Mono.just(Tuples.of(initialRun.getT1(), ieNlpModule));

                        if (!infOnly) {
                            return runService.updateToTrainStatus(initialRun.getT1())
                                    .flatMap(test -> ieNlpModule.performTraining(config, initialRun.getT1()))
                                    .doOnError(trainError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, trainError.getMessage())))
                                    .then(res);
                        }

                        return res;
                    })
                )
                .flatMap(trainRes -> runService.updateToInferenceStatus(trainRes.getT1())
                        .flatMap(updateRes -> trainRes.getT2().performInference(config, trainRes.getT1())
                                .doOnError(infError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, infError.getMessage())))
                                .flatMap(infRes -> moduleService.decrementJobCount(ieNlpModule1.getName())
                                        .flatMap(ignored -> runService.updateToScoringStatus(trainRes.getT1())))
                                .flatMap(updatedRun -> moduleService.buildNlpModuleClient(ModuleService.EVAL_SERVICE_NAME)
                                        .flatMap(evalModule -> {
                                            EvalNlpModule evalNlpModule = (EvalNlpModule) evalModule;
                                            return runService.getInferenceOutput(trainRes.getT1())
                                                    .flatMap(sysFile -> evalNlpModule.performEval(testFile, sysFile, trainRes.getT1())
                                                            .doOnError(evalError -> {
                                                                evalNlpModule.handleErrorDuringRun(evalError, trainRes.getT1());
                                                                Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, evalError.getMessage()));
                                                            }));
                                        }))
                        ).flatMap(ignored -> Mono.just(trainRes.getT1()))
                );
    }
}
