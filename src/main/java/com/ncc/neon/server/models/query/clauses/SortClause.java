package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * SortClause
 */
@Data
@AllArgsConstructor
public class SortClause {

    String fieldName;
    SortOrder sortOrder;

    public int getSortOrder() {
        return sortOrder.direction();
    }
}