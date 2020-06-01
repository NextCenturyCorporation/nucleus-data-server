package com.ncc.neon.models;

import lombok.Data;

@Data
public class ExperimentForm {
    private final String trainFile;
    private final String devFile;
    private final String testFile;
    private final String configFile;
    private final String module;
    private final boolean infOnly;
    private final boolean runEval;
}
