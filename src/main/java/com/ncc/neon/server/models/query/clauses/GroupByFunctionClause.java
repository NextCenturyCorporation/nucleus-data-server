package com.ncc.neon.server.models.query.clauses;

public class GroupByFunctionClause extends FieldFunction implements GroupByClause {
    public GroupByFunctionClause(String name, String operation, String field){
        super(name, operation, field);
    }
}
