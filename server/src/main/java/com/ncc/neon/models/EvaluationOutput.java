package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class EvaluationOutput {
    @JsonProperty("run-ID")
    private String runID;
    @JsonProperty("system-ID")
    private String systemId;
    @JsonProperty("reference_ID")
    private String referenceId;
    @JsonProperty("evt-misses")
    private double evtMisses;
    @JsonProperty("evt-false-alarms")
    private double evtFalseAlarms;
    @JsonProperty("evt-matches")
    private double evtMatches;
    @JsonProperty("evt-precision")
    private double evtPrecision;
    @JsonProperty("evt-recall")
    private double evtRecall;
    @JsonProperty("evt-fmeasure")
    private double evtFmeasure;
    @JsonProperty("arg-misses")
    private double argMisses;
    @JsonProperty("arg-false-alarms")
    private double argFalseAlarms;
    @JsonProperty("arg-matches")
    private double argMatches;
    @JsonProperty("arg-precision")
    private double argPrecision;
    @JsonProperty("arg-fmeasure")
    private double argFmeasure;
    @JsonProperty("combined-score")
    private double combinedScore;
}
