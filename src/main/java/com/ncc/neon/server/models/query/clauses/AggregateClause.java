package com.ncc.neon.server.models.query.clauses;
/**
 * This class does not have an additional implementation from the super class,
 * but its type is used by jackson
 */

public class AggregateClause extends FieldFunction {

    public AggregateClause(String name, String operation, String field) {
        super(name, operation, field);
    }
}