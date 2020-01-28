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
        data.put("trainEndTime", DateUtil.getCurrentDateTime());
        data.put("status", RunStatus.INFERENCING);
        data.put("infStartTime", DateUtil.getCurrentDateTime());
        return update(data, index, dataType, runId);
    }

    public Mono<RestStatus> updateToScoringStatus(String runId) {
        Map<String, Object> data = new HashMap<>();
        data.put("infEndTime", DateUtil.getCurrentDateTime());
        data.put("status", RunStatus.SCORING);
        return update(data, index, dataType, runId);
    }

    public Mono<RestStatus> completeRun(String completedRunId) {
        Map<String, Object> data = new HashMap<>();
        data.put("status", "complete");
        data.put("ended", DateUtil.getCurrentDateTime());
        return update(data, index, dataType, completedRunId);
    }
}
