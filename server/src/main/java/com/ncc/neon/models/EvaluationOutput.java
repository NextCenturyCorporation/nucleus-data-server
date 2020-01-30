package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class EvaluationOutput {
    private final String runID;
    private final Score[] score;
}
