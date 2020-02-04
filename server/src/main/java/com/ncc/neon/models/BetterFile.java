package com.ncc.neon.models;

import lombok.Data;

@Data
public class BetterFile {
    private final String filename;
    private final long bytes;
    private FileStatus status = FileStatus.PENDING;
    private String status_message;
}
