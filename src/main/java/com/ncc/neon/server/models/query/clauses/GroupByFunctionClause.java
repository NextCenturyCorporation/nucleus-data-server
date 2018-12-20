package com.ncc.neon.server.models.query.clauses;

/**
 * GroupByFunctionClause
 */
public class GroupByFunctionClause extends FieldFunction implements GroupByClause {

    public GroupByFunctionClause(String name, String operation, String field){
        super(name, operation, field);
    }
}