package com.ncc.neon.server.models.query.clauses;

import java.util.List;

public class OrWhereClause extends BooleanWhereClause {
    public OrWhereClause(List<WhereClause> whereClauses) {
        super(whereClauses);
    }
}
