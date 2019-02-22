package com.ncc.neon.server.models.query.filter;

import com.ncc.neon.server.models.query.clauses.WhereClause;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Filter {
    String databaseName;
    String tableName;
    String filterName;

    WhereClause whereClause;

    // TODO: look into @RequiredArgsConstructor
    public Filter(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

}