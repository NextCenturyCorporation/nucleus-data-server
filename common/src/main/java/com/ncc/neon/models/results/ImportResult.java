package com.ncc.neon.models.results;

import lombok.Value;

@Value
public class ImportResult {
    int total;
    int failCount;
}