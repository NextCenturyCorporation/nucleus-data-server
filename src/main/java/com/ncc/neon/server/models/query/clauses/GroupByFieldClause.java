package com.ncc.neon.server.models.query.clauses;

import lombok.Data;

/**
 * GroupByFieldClause
 */
@Data
class GroupByFieldClause implements GroupByClause {

    String field;
    String prettyField;

}