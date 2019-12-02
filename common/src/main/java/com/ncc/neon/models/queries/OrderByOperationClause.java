package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderByOperationClause implements OrderByClause {
    String operation;
    Order order;

    @Override
    public String getCompleteFieldOrOperation() {
        return this.getFieldOrOperation();
    }

    @Override
    public String getFieldOrOperation() {
        return this.operation;
    }
}
