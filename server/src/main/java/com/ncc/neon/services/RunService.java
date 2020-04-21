package com.ncc.neon.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.exception.UpsertException;
import com.ncc.neon.models.Run;
import com.ncc.neon.models.RunStatus;
import com.ncc.neon.models.Score;
import com.ncc.neon.util.DateUtil;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class RunService extends ElasticSearchService<Run> {
    private static final String TRAIN_START_TIME_FIELD = "train_start_time";
    private static final String TRAIN_END_TIME_FIELD = "train_end_time";
    private static final String INF_START_TIME_FIELD = "inf_start_time";
    private static final String INF_END_TIME_FIELD = "inf_end_time";
    private static final String STATUS_FIELD = "status";
    private static final String STATUS_MESSAGE_FIELD = "status_message";
    private static final String OVERALL_SCORE_FIELD = "overall_score";

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    RunService(DatasetService datasetService,
               @Value("${db_host}") String dbHost,
               @Value("${run.table}") String runTable) {
        super(dbHost, runTable, runTable, Run.class, datasetService);
    }

    public Mono<String> initRun(String experimentId, Map<String, String> trainConfigParams, Map<String, String> infConfigParams) {
        Run run = new Run(experimentId, trainConfigParams, infConfigParams);
        return insert(run);
    }

    public String initRunSync(String experimentId, Map<String, String> trainConfigParams, Map<String, String> infConfigParams) throws UpsertException {
        Run run = new Run(experimentId, trainConfigParams, infConfigParams);
        return insertSync(run);
    }

    public Mono<RestStatus> updateToTrainStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put(TRAIN_START_TIME_FIELD, DateUtil.getCurrentDateTime());
        data.put(STATUS_FIELD, RunStatus.TRAINING);
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> completeTraining(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put(TRAIN_END_TIME_FIELD, DateUtil.getCurrentDateTime());
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateToInferenceStatus(String runId) {
        String currTime = DateUtil.getCurrentDateTime();
        Map<String, Object> data = new HashMap<>();
        data.put(STATUS_FIELD, RunStatus.INFERENCING);
        data.put(TRAIN_END_TIME_FIELD, currTime);
        data.put(INF_START_TIME_FIELD, currTime);
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateToScoringStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put(INF_END_TIME_FIELD, DateUtil.getCurrentDateTime());
        data.put(STATUS_FIELD, RunStatus.SCORING);
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateToDoneStatus(String completedRunId, Score overallScore) {
        Map<String, Object> data = new HashMap<>();
        Map<String, Object> scoreData = mapper.convertValue(overallScore, Map.class);
        data.put(STATUS_FIELD, RunStatus.DONE);
        data.put(OVERALL_SCORE_FIELD, scoreData);
        return updateAndRefresh(data, completedRunId);
    }

    public Mono<RestStatus> updateToCanceledStatus(String canceledRunId) {
        Map<String, Object> data = new HashMap<>();
        data.put(STATUS_FIELD, RunStatus.CANCELED);
        return updateAndRefresh(data, canceledRunId);
    }

    public Mono<RestStatus> updateToErrorStatus(String runId, String errorMsg) {
        Map<String, Object> data = new HashMap<>();
        data.put(STATUS_FIELD, RunStatus.ERROR);
        data.put(STATUS_MESSAGE_FIELD, errorMsg);
        return updateAndRefresh(data, runId);
    }

    public Mono<RestStatus> updateOutputs(String runId, String outputsKey, String[] outputs) {
        Map<String, Object> data = new HashMap<>();
        data.put(outputsKey, outputs);
        return updateAndRefresh(data, runId);
    }

    public Mono<String> getInferenceOutput(String runId) {
        return getById(runId)
            .map(run -> run.getInfOutputs()[0]);

    }

    public Boolean isCanceledSync(String runId) throws IOException {
        Run run  = getByIdSync(runId);
        return run.getStatus() == RunStatus.CANCELED;
    }
}
