package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Experiment {
    public static final String CURR_RUN_KEY = "curr_run";

    private final String name;
    @JsonProperty("total_runs")
    private final int totalRuns;
    @JsonProperty("best_score")
    private Double bestScore;
    @JsonProperty("best_run_id")
    private String bestRunId;
    @JsonProperty("worst_score")
    private Double worstScore;
    @JsonProperty("worst_run_id")
    private String worstRunId;
    @JsonProperty(CURR_RUN_KEY)
    private int currRun;
    @JsonProperty("start_time")
    private String startTime;
    @JsonProperty("end_time")
    private String endTime;
}
