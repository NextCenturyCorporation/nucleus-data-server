package com.ncc.neon.server.models.query.clauses;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class BooleanWhereClause implements WhereClause {
    List<WhereClause> whereClauses;
}
