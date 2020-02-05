package com.ncc.neon.models;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import lombok.Value;

@Value
public class DatabaseOperationErrorResponse {
    private final String id;
    private final String message;
}
