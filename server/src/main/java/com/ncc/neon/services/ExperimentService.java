package com.ncc.neon.services;

import com.ncc.neon.better.ExperimentConfig;
import com.ncc.neon.models.Experiment;
import com.ncc.neon.models.Run;
import com.ncc.neon.util.DateUtil;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static com.ncc.neon.models.Experiment.*;

@Component
public class ExperimentService extends ElasticSearchService<Experiment> {
    private RunService runService;

    @Autowired
    ExperimentService(DatasetService datasetService,
                      RunService runService,
                      @Value("${db_host}") String dbHost,
                      @Value("${experiment.table}") String experimentTable) {
        super(dbHost, experimentTable, experimentTable, Experiment.class, datasetService);
        this.runService = runService;
    }

    public Mono<Tuple2<String, RestStatus>> insertNew(ExperimentConfig config) {
        Experiment experiment = new Experiment(config.getName(), config.getEvalConfigs().length);
        return insert(experiment);
    }

    public Mono<RestStatus> incrementCurrRun(String experimentId) {
        return getById(experimentId)
                .flatMap(experiment -> {
                    // Increment run count.
                    experiment.setCurrRun(experiment.getCurrRun() + 1);
                    Map<String, Object> data = new HashMap<>();
                    data.put(CURR_RUN_KEY, experiment.getCurrRun());
                    return updateAndRefresh(data, experimentId);
                });
    }

    public Mono<RestStatus> updateOnRunComplete(String completedRunId, String experimentId) {
        return getById(experimentId)
                .flatMap(experiment -> runService.getById(completedRunId)
                    .flatMap(completedRun -> {
                    Map<String, Object> data = new HashMap<>();

                    Double currScore = completedRun.getOverallScore().getCombinedScore();

                    // Check for a new best score.
                    if (currScore >= experiment.getBestScore()) {
                        data.put(BEST_SCORE_KEY, currScore);
                        data.put(BEST_RUN_ID_KEY, completedRunId);
                    }

                    // Check for a new worst score.
                    if (currScore <= experiment.getWorstScore()) {
                        data.put(WORST_SCORE_KEY, currScore);
                        data.put(WORST_RUN_ID_KEY, completedRunId);
                    }

                    // Check if we are done with all the runs.
                    if (experiment.getCurrRun() == experiment.getTotalRuns()) {
                        data.put(END_TIME_KEY, DateUtil.getCurrentDateTime());
                    }

                    return updateAndRefresh(data, experimentId);
                }));
    }


}
