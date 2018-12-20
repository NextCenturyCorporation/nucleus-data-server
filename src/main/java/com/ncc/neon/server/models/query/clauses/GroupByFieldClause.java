package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * GroupByFieldClause
 */
@Data
@AllArgsConstructor
public class GroupByFieldClause implements GroupByClause {

    String field;
    String prettyField;

}