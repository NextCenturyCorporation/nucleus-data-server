package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderByFieldClause implements OrderByClause {
    FieldClause fieldClause;
    Order order;

    @Override
    public String getCompleteFieldOrGroup() {
        return this.fieldClause.getComplete();
    }

    @Override
    public String getFieldOrGroup() {
        return this.fieldClause.getField();
    }
}
