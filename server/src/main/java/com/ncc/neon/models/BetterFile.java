package com.ncc.neon.models;

import lombok.Data;

@Data
public class BetterFile {
    private final String filename;
    // TODO: Set to long type.
    private final int bytes;
}
