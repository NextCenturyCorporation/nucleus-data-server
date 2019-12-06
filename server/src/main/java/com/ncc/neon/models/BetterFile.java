package com.ncc.neon.models;

public class BetterFile {
    int bytes;
    String filename;
    String id;

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

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
