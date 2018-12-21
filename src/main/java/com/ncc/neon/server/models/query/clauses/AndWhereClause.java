package com.ncc.neon.server.models.query.clauses;

import java.util.List;

public class AndWhereClause extends BooleanWhereClause {
    public AndWhereClause(List<WhereClause> whereClauses) {
        super(whereClauses);
    }
}
