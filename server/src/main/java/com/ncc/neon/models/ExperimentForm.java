package com.ncc.neon.models;

import lombok.Data;

@Data
public class ExperimentForm {
    private String trainFile;
    private String devFile;
    private String testFile;
    private String configFile;
    private String module;
    private boolean infOnly;
}
