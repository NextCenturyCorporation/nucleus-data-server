package com.ncc.neon.server.models.query.clauses;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = AndWhereClause.class, name = "and"),
    @JsonSubTypes.Type(value = OrWhereClause.class, name = "or"),
    @JsonSubTypes.Type(value = SingularWhereClause.class, name = "where")
})
public interface WhereClause {
}
