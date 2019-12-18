package com.ncc.neon.models;

import lombok.Data;

import java.io.File;

@Data
public class BetterFile {
    private final String filename;
    private final long bytes;

    public BetterFile(File fileRef) {
        this.filename = fileRef.getName();
        this.bytes = fileRef.length();
    }
}
