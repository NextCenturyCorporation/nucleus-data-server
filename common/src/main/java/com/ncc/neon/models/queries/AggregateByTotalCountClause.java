package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AggregateByTotalCountClause implements AggregateClause {
    String label;

    @Override
    public String getOperation() {
        return "count";
    }
}
