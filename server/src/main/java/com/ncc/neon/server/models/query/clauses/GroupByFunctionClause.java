package com.ncc.neon.server.models.query.clauses;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper=false)
@NoArgsConstructor
public class GroupByFunctionClause extends FieldFunction implements GroupByClause {
    public GroupByFunctionClause(String name, String operation, String field) {
        super(name, operation, field);
    }
}
