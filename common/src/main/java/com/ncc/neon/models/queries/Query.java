package com.ncc.neon.models.queries;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ncc.neon.models.queries.AggregateClause;
import com.ncc.neon.models.queries.Filter;
import com.ncc.neon.models.queries.GroupByClause;
import com.ncc.neon.models.queries.LimitClause;
import com.ncc.neon.models.queries.OffsetClause;
import com.ncc.neon.models.queries.SelectClause;
import com.ncc.neon.models.queries.SortClause;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
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
