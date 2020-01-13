package com.ncc.neon.exception;

public class UpsertException extends Exception {
    public UpsertException(String filename) {
        super("Failed to add file to database: " + filename);
    }
}
