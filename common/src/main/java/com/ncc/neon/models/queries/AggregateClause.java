package com.ncc.neon.models.queries;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = AggregateByFieldClause.class, name = "field"),
    @JsonSubTypes.Type(value = AggregateByGroupCountClause.class, name = "group"),
    @JsonSubTypes.Type(value = AggregateByTotalCountClause.class, name = "total")
})
public interface AggregateClause {
    public String getLabel();
    public String getOperation();
}
