package com.ncc.neon.server.models.query;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
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

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Query {
    Filter filter;
    boolean aggregateArraysByElement = false;

    @JsonProperty(value = "isDistinct")
    boolean isDistinct = false;

    List<String> fields = SelectClause.ALL_FIELDS;
    List<AggregateClause> aggregates = new ArrayList<>();
    List<GroupByClause> groupByClauses = new ArrayList<>();
    List<SortClause> sortClauses = new ArrayList<>();
    LimitClause limitClause;
    OffsetClause offsetClause;
}