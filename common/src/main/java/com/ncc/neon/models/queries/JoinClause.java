package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class JoinClause {
    String type;
    String database;
    String table;
    WhereClause onClause;
}
