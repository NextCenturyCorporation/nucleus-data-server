package com.ncc.neon.server.models.query.clauses;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface just to give context that implementors are WhereClauses.
 * Also provides JSON metadata to determine which implementation to use
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
// TODO: fix this
@JsonSubTypes({
        /*
         * @JsonSubTypes.Type(value = SingularWhereClause, name = "where"),
         * 
         * @JsonSubTypes.Type(value = WithinDistanceClause, name = "withinDistance"),
         * 
         * @JsonSubTypes.Type(value = GeoIntersectionClause, name = "geoIntersection"),
         * 
         * @JsonSubTypes.Type(value = GeoWithinClause, name = "geoWithin"),
         * 
         * @JsonSubTypes.Type(value = AndWhereClause, name = "and"),
         * 
         * @JsonSubTypes.Type(value = OrWhereClause, name = "or")
         */
})
public interface WhereClause {
}