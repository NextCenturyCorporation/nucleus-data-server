package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A generic function that can be applied to a database field that results in a
 * new field being created (such as creating a new field based on the sum of
 * other fields)
 */
@Data
@AllArgsConstructor
public class FieldFunction {
    String name;
    String operation;
    String field;
}