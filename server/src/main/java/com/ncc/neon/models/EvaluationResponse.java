package com.ncc.neon.models;

import lombok.Data;

@Data
public class EvaluationResponse {
    private final Score[] evaluation;
    private final BetterFile[] files;
}
