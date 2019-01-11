package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GroupByFieldClause implements GroupByClause {
    String field;
    String prettyField;
}
