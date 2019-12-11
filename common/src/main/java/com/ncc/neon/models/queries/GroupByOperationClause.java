package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class GroupByOperationClause implements GroupByClause {
    FieldClause fieldClause;
    String label;
    String operation;

    @Override
    public String getCompleteField() {
        return this.fieldClause.getComplete();
    }

    @Override
    public String getField() {
        return this.fieldClause.getField();
    }
}
