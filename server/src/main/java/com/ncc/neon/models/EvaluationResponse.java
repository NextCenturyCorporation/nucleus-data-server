package com.ncc.neon.models;

import lombok.Data;

@Data
public class EvaluationResponse {
    private final EvaluationOutput evaluationOutput;
    private final BetterFile[] outputFiles;
}
