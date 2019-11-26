package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AggregateByFieldClause implements AggregateClause {
    FieldClause fieldClause;
    String label;
    String operation;

    public String getCompleteField() {
        return this.fieldClause.getComplete();
    }

    public String getField() {
        return this.fieldClause.getField();
    }
}
