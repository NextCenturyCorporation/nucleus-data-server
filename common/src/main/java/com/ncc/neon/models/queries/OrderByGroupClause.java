package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderByGroupClause implements OrderByClause {
    String group;
    Order order;

    @Override
    public String getCompleteFieldOrGroup() {
        return this.getFieldOrGroup();
    }

    @Override
    public String getFieldOrGroup() {
        return this.group;
    }
}
