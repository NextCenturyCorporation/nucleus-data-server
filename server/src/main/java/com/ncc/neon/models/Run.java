package com.ncc.neon.models;

import lombok.Data;

@Data
public class Run {
    private final String configuration;
    private final String[] outputs;
    private final String started;
    private String ended;
    private final String status;
}
