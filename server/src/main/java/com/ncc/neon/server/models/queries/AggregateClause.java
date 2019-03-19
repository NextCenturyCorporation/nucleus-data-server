package com.ncc.neon.server.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AggregateClause {
    String name;
    String operation;
    String field;
}
