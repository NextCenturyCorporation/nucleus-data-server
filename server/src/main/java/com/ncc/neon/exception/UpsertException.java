package com.ncc.neon.exception;

public class UpsertException extends Exception {
    private String rowId;

    public UpsertException(String id, String message) {
        super(message);
        rowId = id;
    }
}
