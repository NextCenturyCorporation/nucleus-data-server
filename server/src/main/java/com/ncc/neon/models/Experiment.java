package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ncc.neon.util.DateUtil;
import lombok.Data;

@Data
public class Experiment {
    public static final String CURR_RUN_KEY = "curr_run";
    public static final String BEST_SCORE_KEY = "best_score";
    public static final String BEST_RUN_ID_KEY = "best_run_id";
    public static final String WORST_SCORE_KEY = "worst_score";
    public static final String WORST_RUN_ID_KEY = "worst_run_id";
    public static final String END_TIME_KEY = "end_time";

    private final String name;
    @JsonProperty("total_runs")
    private final int totalRuns;
    @JsonProperty(BEST_SCORE_KEY)
    private Double bestScore = 0.0;
    @JsonProperty(BEST_RUN_ID_KEY)
    private String bestRunId;
    @JsonProperty(WORST_SCORE_KEY)
    private Double worstScore = 0.0;
    @JsonProperty(WORST_RUN_ID_KEY)
    private String worstRunId;
    @JsonProperty(CURR_RUN_KEY)
    private int currRun;
    @JsonProperty("start_time")
    private String startTime = DateUtil.getCurrentDateTime();
    @JsonProperty(END_TIME_KEY)
    private String endTime;
}
