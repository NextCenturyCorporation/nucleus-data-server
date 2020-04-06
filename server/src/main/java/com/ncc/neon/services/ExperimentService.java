package com.ncc.neon.services;

import com.ncc.neon.better.ExperimentConfig;
import com.ncc.neon.models.Experiment;
import com.ncc.neon.models.Run;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static com.ncc.neon.models.Experiment.CURR_RUN_KEY;

@Component
public class ExperimentService extends ElasticSearchService<Experiment> {
    @Autowired
    ExperimentService(DatasetService datasetService,
                      @Value("${db_host}") String dbHost,
                      @Value("${experiment.table}") String experimentTable) {
        super(dbHost, experimentTable, experimentTable, Experiment.class, datasetService);
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

    public Mono<RestStatus> updateOnRunComplete(Run completedRun, String experimentId) {
        return getById(experimentId)
                .flatMap(experiment -> {
                    // TODO: Find running min and max scores and corresponding run IDs.
                    Map<String, Object> data = new HashMap<>();
                    return updateAndRefresh(data, experimentId);
                });
    }
}
