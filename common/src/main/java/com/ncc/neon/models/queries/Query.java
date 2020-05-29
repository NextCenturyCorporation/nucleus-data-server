package com.ncc.neon.models.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ncc.neon.models.queries.AggregateClause;
import com.ncc.neon.models.queries.GroupByClause;
import com.ncc.neon.models.queries.LimitClause;
import com.ncc.neon.models.queries.OffsetClause;
import com.ncc.neon.models.queries.SelectClause;
import com.ncc.neon.models.queries.OrderByClause;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class Query {
    SelectClause selectClause;
    WhereClause whereClause;
    ClusterClause clusterClause;
    List<AggregateClause> aggregateClauses = new ArrayList<>();
    List<GroupByClause> groupByClauses = new ArrayList<>();
    List<OrderByClause> orderByClauses = new ArrayList<>();
    LimitClause limitClause;
    OffsetClause offsetClause;
    List<JoinClause> joinClauses = new ArrayList<>();
    String rawQuery;
    Map<String, Object> rawQueryMetadata;

    @JsonProperty(value = "isDistinct")
    boolean isDistinct = false;
}
