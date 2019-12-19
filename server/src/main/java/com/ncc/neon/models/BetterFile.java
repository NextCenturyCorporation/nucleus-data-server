package com.ncc.neon.models;

import lombok.Data;

import java.io.File;

@Data
public class BetterFile {
    private final String filename;
    private final long bytes;
}
