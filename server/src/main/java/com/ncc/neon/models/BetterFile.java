package com.ncc.neon.models;

public class BetterFile {
    int bytes;
    String filename;

    public BetterFile() {
        super();
    }

    public BetterFile(int bytes, String filename) {
        this.bytes = bytes;
        this.filename = filename;
    }

    public int getBytes() {
        return bytes;
    }

    public String getFilename() {
        return filename;
    }
}
