package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SortClause {
    String fieldName;
    SortOrder sortOrder;

    public int getSortDirection() {
        return sortOrder.getDirection();
    }
}
