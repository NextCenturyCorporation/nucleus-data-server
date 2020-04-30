package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Run {
    public static final String EXPERIMENT_ID_KEY = "experiment_id";
    public static final String STATUS_KEY = "status";

    @JsonProperty(EXPERIMENT_ID_KEY)
    private final String experimentId;
    @JsonProperty("train_config")
    private final String trainConfig;
    @JsonProperty("inf_config")
    private final String infConfig;
    @JsonProperty("train_outputs")
    private String[] trainOutputs;
    @JsonProperty("inf_outputs")
    private String[] infOutputs;
    @JsonProperty("eval_outputs")
    private String[] evalOutputs;
    @JsonProperty("train_start_time")
    private String trainStartTime;
    @JsonProperty("train_end_time")
    private String trainEndTime;
    @JsonProperty("inf_start_time")
    private String infStartTime;
    @JsonProperty("inf_end_time")
    private String infEndTime;
    @JsonProperty("overall_score")
    private Score overallScore;
    @JsonProperty(STATUS_KEY)
    private RunStatus status;
    @JsonProperty("status_message")
    private String statusMessage;
}
