package com.ncc.neon.models.queries;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = GroupByFieldClause.class, name = "field"),
    @JsonSubTypes.Type(value = GroupByOperationClause.class, name = "operation")
})
public interface GroupByClause {
    public String getCompleteField();
    public String getField();
}
