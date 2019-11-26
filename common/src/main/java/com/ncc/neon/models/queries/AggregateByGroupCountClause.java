package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AggregateByGroupCountClause implements AggregateClause {
    String group;
    String label;

    @Override
    public String getOperation() {
        return "count";
    }
}
