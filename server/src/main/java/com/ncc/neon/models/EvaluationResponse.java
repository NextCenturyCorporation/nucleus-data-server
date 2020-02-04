package com.ncc.neon.models;

import lombok.Data;

@Data
public class EvaluationResponse {
    private final Score[] evaluation;
    private final BetterFile[] files;

    public double getOverallScore() {
        return evaluation[evaluation.length - 1].getCombinedScore();
    }
}
