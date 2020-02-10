package com.ncc.neon.services;

import com.ncc.neon.better.EvalNlpModule;
import com.ncc.neon.better.IENlpModule;
import com.ncc.neon.better.PreprocessorNlpModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Component
public class AsyncService {

    private NlpModuleService nlpModuleService;
    private RunService runService;

    @Autowired
    public AsyncService(NlpModuleService nlpModuleService, RunService runService) {
        this.nlpModuleService = nlpModuleService;
        this.runService = runService;
    }

    public Mono<?> processEvaluation(String trainConfigFile, String infConfigFile, String module,
                                  boolean infOnly,String refFile) {
        return nlpModuleService.getNlpModule(module)
                .flatMap(nlpModule -> runService.initRun(trainConfigFile, infConfigFile)
                        .flatMap(initialRun -> {
                            IENlpModule ieNlpModule = (IENlpModule) nlpModule;
                            Mono<Tuple2<String, IENlpModule>> res = Mono.just(Tuples.of(initialRun.getT1(), ieNlpModule));

                            if (!infOnly) {
                                return runService.updateToTrainStatus(initialRun.getT1())
                                        .flatMap(test -> ieNlpModule.performTraining(trainConfigFile, initialRun.getT1()))
                                        .doOnError(trainError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, trainError.getMessage())))
                                        .then(res);
                            }

                            return res;
                        }))
                .flatMap(trainRes -> runService.updateToInferenceStatus(trainRes.getT1())
                        .flatMap(updateRes -> trainRes.getT2().performInference(infConfigFile, trainRes.getT1())
                                .doOnError(infError -> Flux.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, infError.getMessage())))
                                .flatMap(infRes -> runService.updateToScoringStatus(trainRes.getT1()))
                                .flatMap(updatedRun -> nlpModuleService.getNlpModule("ie_eval")
                                        .flatMap(evalModule -> {
                                            EvalNlpModule evalNlpModule = (EvalNlpModule) evalModule;
                                            return runService.getInferenceOutput(trainRes.getT1())
                                                    .flatMap(sysFile -> evalNlpModule.performEval(refFile, sysFile, trainRes.getT1())
                                                            .doOnError(evalError -> Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, evalError.getMessage()))));
                                        }))
                        )
                );
    }

    public Flux<?> processTraining(String configFile, String module, String runId) {
        return nlpModuleService.getNlpModule(module).flatMapMany(nlpModule -> {
            IENlpModule trainNlpModule = (IENlpModule) nlpModule;
            return trainNlpModule.performTraining(configFile, runId);
        });
    }

    public Flux<?> performPreprocess(String file, String module) {
        return nlpModuleService.getNlpModule(module).flatMapMany(nlpModule -> {
            PreprocessorNlpModule preprocessorNlpModule = (PreprocessorNlpModule)nlpModule;
            return preprocessorNlpModule.performPreprocessing(file);
        });
    }
}
