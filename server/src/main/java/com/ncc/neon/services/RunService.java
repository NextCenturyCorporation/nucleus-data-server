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
    private static final String index = "run";
    private static final String dataType = "run";

    @Autowired
    RunService(@Value("${db_host}") String dbHost) {
        super(dbHost, index, dataType, Run.class);
    }

    public Mono<Tuple2<String, RestStatus>> initRun(String configFile) {
        Run run = new Run(configFile);
        return insert(run);
    }

    public Mono<RestStatus> updateToInferenceStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put("train_end_time", DateUtil.getCurrentDateTime());
        data.put("status", RunStatus.INFERENCING);
        data.put("inf_start_time", DateUtil.getCurrentDateTime());
        return update(data, runId);
    }

    public Mono<RestStatus> updateToScoringStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put("inf_end_time", DateUtil.getCurrentDateTime());
        data.put("status", RunStatus.SCORING);
        return update(data, runId);
    }

    public Mono<RestStatus> updateToDoneStatus(String completedRunId) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", RunStatus.DONE);
        return update(data, completedRunId);
    }

    public Mono<RestStatus> updateToErrorStatus(String runId, String errorMsg) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", RunStatus.ERROR);
        data.put("status_message", errorMsg);
        return update(data, runId);
    }

    public Mono<RestStatus> updateOutputs(String runId, String outputsKey, String[] outputs) {
        Map<String, Object> data = new HashMap<>();
        data.put(outputsKey, outputs);
        return update(data, runId);
    }
}
