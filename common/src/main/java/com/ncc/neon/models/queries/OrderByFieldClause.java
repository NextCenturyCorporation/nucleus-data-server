package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderByFieldClause implements OrderByClause {
    FieldClause fieldClause;
    Order order;

    @Override
    public String getCompleteFieldOrOperation() {
        return this.fieldClause.getComplete();
    }

    @Override
    public String getFieldOrOperation() {
        return this.fieldClause.getField();
    }
}
