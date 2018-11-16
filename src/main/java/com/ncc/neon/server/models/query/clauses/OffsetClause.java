package com.ncc.neon.server.models.query.clauses;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * LimitClause
 */
@Data
@Accessors(fluent = true)
public class OffsetClause {

    int limit;
}