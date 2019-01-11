package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SingularWhereClause implements WhereClause {
    String lhs;
    String operator;
    String rhs;
}
