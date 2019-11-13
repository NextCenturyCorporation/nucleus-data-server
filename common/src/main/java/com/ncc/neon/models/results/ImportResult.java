package com.ncc.neon.models.results;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class ImportResult {
    int total;
    int failCount;
    String error;

    public ImportResult(int total, int failCount) {
        this.total = total;
        this.failCount = failCount;
        this.error = "";
    }
}