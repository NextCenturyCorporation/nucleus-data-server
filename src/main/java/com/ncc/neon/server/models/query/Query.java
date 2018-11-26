package com.ncc.neon.server.models.query;

import java.util.List;

import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.GroupByClause;
import com.ncc.neon.server.models.query.clauses.LimitClause;
import com.ncc.neon.server.models.query.clauses.OffsetClause;
import com.ncc.neon.server.models.query.clauses.SelectClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.filter.Filter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Query
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Query {
    Filter filter;
    boolean aggregateArraysByElement = false;
    boolean isDistinct = false;
    List<String> fields = SelectClause.ALL_FIELDS;
    // new ArrayList<>(List.of())
    List<AggregateClause> aggregates = List.of();
    List<GroupByClause> groupByClauses = List.of();
    List<SortClause> sortClauses = List.of();
    LimitClause limitClause;
    OffsetClause offsetClause;
}