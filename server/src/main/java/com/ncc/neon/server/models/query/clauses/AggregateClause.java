package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
@AllArgsConstructor
public class AggregateClause extends FieldFunction {
    public AggregateClause(String name, String operation, String field) {
        super(name, operation, field);
    }
}
