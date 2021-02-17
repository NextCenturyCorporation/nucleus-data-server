package com.ncc.neon.models;

import lombok.Data;

@Data
public class IRExperimentForm {
    private final String taskFile;
    private final String corpus;
    private final String module;
}
