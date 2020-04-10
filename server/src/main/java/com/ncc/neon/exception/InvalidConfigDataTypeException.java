package com.ncc.neon.exception;

public class InvalidConfigDataTypeException extends RuntimeException {
    public InvalidConfigDataTypeException(String invalidDatatype) {
        super("Invalid config datatype.  Must be String, was " + invalidDatatype);
    }
}
