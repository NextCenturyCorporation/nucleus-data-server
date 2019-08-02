package com.ncc.neon.models.queries;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class SortClause {
    String fieldName;
    SortClauseOrder sortOrder;

    @JsonIgnore
    public int getSortDirection() {
        return sortOrder.getDirection();
    }
}
