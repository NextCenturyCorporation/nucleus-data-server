package com.ncc.neon.services;

import com.ncc.neon.better.*;
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
    private RunService runService;

    @Autowired
    private Environment env;

    @Autowired
    public AsyncService(ModuleService moduleService, RunService runService) {
        this.moduleService = moduleService;
        this.runService = runService;
    }

    public Mono<Object> processExperiment(ExperimentConfig experimentConfig, boolean infOnly) {
        return moduleService.buildNlpModuleClient(experimentConfig.module)
                .flatMap(nlpModule -> Flux.fromArray(experimentConfig.getEvalConfigs())
                    // Sequential maintains source ordering with a specified max concurrency.
                    .flatMapSequential(config -> moduleService.incrementJobCount(experimentConfig.module)
                        .flatMap(ignored -> runService.initRun(config.getTrainConfigFilename(), config.getInfConfigFilename())
                            .flatMap(initialRun -> {
                                IENlpModule ieNlpModule = (IENlpModule) nlpModule;

                                Mono<Tuple2<String, IENlpModule>> res = Mono.just(Tuples.of(initialRun.getT1(), ieNlpModule));

                                if (!infOnly) {
                                    return runService.updateToTrainStatus(initialRun.getT1())
                                            .flatMap(test -> ieNlpModule.performTraining(config, initialRun.getT1()))
                                            .doOnError(trainError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, trainError.getMessage())))
                                            .then(res);
                                }

                                return res;
                            })
                        ).flatMap(trainRes -> runService.updateToInferenceStatus(trainRes.getT1())
                                    .flatMap(updateRes -> trainRes.getT2().performInference(config, trainRes.getT1())
                                            .doOnError(infError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, infError.getMessage())))
                                            .flatMap(infRes -> moduleService.decrementJobCount(experimentConfig.module)
                                                    .flatMap(ignored -> runService.updateToScoringStatus(trainRes.getT1())))
                                            .flatMap(updatedRun -> moduleService.buildNlpModuleClient(ModuleService.EVAL_SERVICE_NAME)
                                                    .flatMap(evalModule -> {
                                                        EvalNlpModule evalNlpModule = (EvalNlpModule) evalModule;
                                                        return runService.getInferenceOutput(trainRes.getT1())
                                                                .flatMap(sysFile -> evalNlpModule.performEval(experimentConfig.getTestFile(), sysFile, trainRes.getT1())
                                                                        .doOnError(evalError -> Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, evalError.getMessage()))));
                                                    }))
                                    )
                            )
                    // Notice max concurrency argument.
                    , Integer.parseInt(Objects.requireNonNull(env.getProperty("server_gpu_count")))).collectList()
                );
    }

    public Mono<?> performPreprocess(String file, String module) {
        return moduleService.buildNlpModuleClient(module).flatMap(nlpModule -> {
            PreprocessorNlpModule preprocessorNlpModule = (PreprocessorNlpModule)nlpModule;

            return moduleService.incrementJobCount(nlpModule.getName())
                    .flatMap(ignored -> preprocessorNlpModule.performPreprocessing(file))
                    .flatMap(ignored -> moduleService.decrementJobCount(nlpModule.getName()));
        });
    }
}
