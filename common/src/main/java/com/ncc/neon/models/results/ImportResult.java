package com.ncc.neon.models.results;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ImportResult {
    String error;
    List<String> recordErrors;

    public ImportResult(List<String> recordErrors) {
        this.recordErrors = recordErrors;
    }

    public ImportResult(String error)
    {
        this.error = error;
    }
}