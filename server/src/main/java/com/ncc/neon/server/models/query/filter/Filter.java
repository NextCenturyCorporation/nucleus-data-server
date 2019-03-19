package com.ncc.neon.server.models.query.filter;

import com.ncc.neon.server.models.query.clauses.WhereClause;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class Filter {
    String databaseName;
    String tableName;
    String filterName;
    WhereClause whereClause;

    public Filter(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }
}