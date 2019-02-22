package com.ncc.neon.server.models.query.clauses;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
@AllArgsConstructor
public class AndWhereClause extends BooleanWhereClause {
    public AndWhereClause(List<WhereClause> whereClauses) {
        super(whereClauses);
    }
}
