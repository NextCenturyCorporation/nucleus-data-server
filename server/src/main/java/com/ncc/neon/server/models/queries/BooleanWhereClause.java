package com.ncc.neon.server.models.queries;

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
