package com.ncc.neon.models.results;

import lombok.Value;

@Value
public class ExportResult {
    String fileName;
    Object data;
}