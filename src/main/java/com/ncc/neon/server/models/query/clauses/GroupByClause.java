package com.ncc.neon.server.models.query.clauses;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * GroupByClause
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = GroupByFieldClause.class, name = "single"),
        @JsonSubTypes.Type(value = GroupByFunctionClause.class, name = "function") })
public interface GroupByClause {
}