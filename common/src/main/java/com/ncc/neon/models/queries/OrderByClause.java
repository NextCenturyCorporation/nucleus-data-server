package com.ncc.neon.models.queries;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderByFieldClause.class, name = "field"),
    @JsonSubTypes.Type(value = OrderByOperationClause.class, name = "operation")
})
public interface OrderByClause {
    public String getCompleteFieldOrOperation();
    public String getFieldOrOperation();
    public Order getOrder();
}
