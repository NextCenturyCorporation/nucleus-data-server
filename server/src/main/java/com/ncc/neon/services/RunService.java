package com.ncc.neon.services;

import com.ncc.neon.models.Run;
import com.ncc.neon.models.RunStatus;
import com.ncc.neon.util.DateUtil;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.Map;

@Component
public class RunService extends ElasticSearchService<Run> {
    @Autowired
    RunService(DatasetService datasetService,
               @Value("${db_host}") String dbHost,
               @Value("${run.table}") String runTable) {
        super(dbHost, runTable, runTable, Run.class, datasetService);
    }

    public Mono<Tuple2<String, RestStatus>> initRun(String trainConfigFile, String infConfigFile) {
        Run run = new Run(trainConfigFile, infConfigFile);
        return insert(run);
    }

    public Mono<RestStatus> updateToTrainStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put("train_start_time", DateUtil.getCurrentDateTime());
        data.put("status", RunStatus.TRAINING);
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> completeTraining(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put("train_end_time", DateUtil.getCurrentDateTime());
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateToInferenceStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", RunStatus.INFERENCING);
        data.put("inf_start_time", DateUtil.getCurrentDateTime());
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateToScoringStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put("inf_end_time", DateUtil.getCurrentDateTime());
        data.put("status", RunStatus.SCORING);
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateToDoneStatus(String completedRunId, double overallScore) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", RunStatus.DONE);
        data.put("overall_score", overallScore);
        return updateAndRefresh(data, completedRunId);
    }

    public Mono<RestStatus> updateToErrorStatus(String runId, String errorMsg) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", RunStatus.ERROR);
        data.put("status_message", errorMsg);
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateOutputs(String runId, String outputsKey, String[] outputs) {
        Map<String, Object> data = new HashMap<>();
        data.put(outputsKey, outputs);
        return updateAndRefresh(data, runId);
    }

    public Mono<String> getInferenceOutput(String runId) {
        return getById(runId)
            .map(run -> run.getInf_outputs()[0]);

    }
}
