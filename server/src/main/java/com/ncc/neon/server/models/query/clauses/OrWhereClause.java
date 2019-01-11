package com.ncc.neon.server.models.query.clauses;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrWhereClause extends BooleanWhereClause {
    public OrWhereClause(List<WhereClause> whereClauses) {
        super(whereClauses);
    }
}
