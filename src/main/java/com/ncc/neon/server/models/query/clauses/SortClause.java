package com.ncc.neon.server.models.query.clauses;

import lombok.Data;

/**
 * SortClause
 */
@Data
public class SortClause {

    String fieldName;
    SortOrder sortOrder;

    public int getSortOrder() {
        return sortOrder.direction();
    }
}