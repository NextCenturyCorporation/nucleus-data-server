package com.ncc.neon.server.models.query.clauses;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SortClause {
    String fieldName;
    SortOrder sortOrder;

    @JsonIgnore
    public int getSortDirection() {
        return sortOrder.getDirection();
    }
}
