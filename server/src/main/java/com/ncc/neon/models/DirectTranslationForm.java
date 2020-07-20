package com.ncc.neon.models;

import lombok.Data;

@Data
public class DirectTranslationForm {
    private final String trainFile;
    private final String devFile;
    private final String testFile;
    private final String resultFile;
    private final String module;
}
