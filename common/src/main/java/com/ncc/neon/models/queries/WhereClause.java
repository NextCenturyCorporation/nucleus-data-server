package com.ncc.neon.models.queries;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = AndWhereClause.class, name = "and"),
    @JsonSubTypes.Type(value = OrWhereClause.class, name = "or"),
    @JsonSubTypes.Type(value = SingularWhereClause.class, name = "where"),
    @JsonSubTypes.Type(value = FieldsWhereClause.class, name = "fields")
})
public interface WhereClause {
}
