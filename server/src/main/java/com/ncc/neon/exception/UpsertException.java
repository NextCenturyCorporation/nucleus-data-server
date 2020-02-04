package com.ncc.neon.exception;

public class UpsertException extends Exception {
    public UpsertException(String id) {
        super("Failed to add item to database: " + id);
    }
}
