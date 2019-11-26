package com.ncc.neon.models.queries;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderByFieldClause.class, name = "field"),
    @JsonSubTypes.Type(value = OrderByGroupClause.class, name = "group")
})
public interface OrderByClause {
    public String getCompleteFieldOrGroup();
    public String getFieldOrGroup();
    public Order getOrder();
}
