package com.ncc.neon.server.models.query.clauses;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * LimitClause
 */
@Data
@AllArgsConstructor
//@Accessors(fluent = true)
public class LimitClause {

    int limit;
}