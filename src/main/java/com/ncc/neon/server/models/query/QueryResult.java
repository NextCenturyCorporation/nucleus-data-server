package com.ncc.neon.server.models.query;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * QueryResult
 */
public interface QueryResult<T> {
   T getData();
}