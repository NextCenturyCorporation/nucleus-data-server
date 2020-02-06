package com.ncc.neon.models.results;

import java.util.List;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ActionResult {
    String error;
    String success;
    List<String> recordErrors;

    public ActionResult(String success, List<String> recordErrors) {
        this.success = success;
        this.recordErrors = recordErrors;
    }

    public ActionResult(String error) {
        this.error = error;
    }
}