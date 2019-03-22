package com.ncc.neon.server.models.queries;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = GroupByFieldClause.class, name = "single"),
    @JsonSubTypes.Type(value = GroupByFunctionClause.class, name = "function")
})
public interface GroupByClause {
}
