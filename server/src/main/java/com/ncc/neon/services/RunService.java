package com.ncc.neon.services;

import com.ncc.neon.models.Run;
import org.elasticsearch.rest.RestStatus;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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

    public Mono<Tuple2<String, RestStatus>> initRun(String configFile, String[] outputFiles) {
        Run run = new Run(configFile, outputFiles, getCurrentDateTime(), "in progress");
        return insert(run);
    }

    public Mono<RestStatus> completeRun(String completedRunId) {
        Map<String, Object> data = new HashMap<>();
        data.put("ended", getCurrentDateTime());
        return update(data, index, dataType, completedRunId);
    }

    private String getCurrentDateTime() {
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSSZ");
        return fmt.print(new DateTime());
    }
}
