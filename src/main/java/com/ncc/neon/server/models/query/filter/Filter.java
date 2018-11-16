package com.ncc.neon.server.models.query.filter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Filter - A filter is applied to a DataSet and can contain a whereClause
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(fluent = true)
public class Filter {
    String databaseName;
    String tableName;
    String filterName;

    // TODO: where clause
    // WhereClause whereClause

    // TODO: look into @RequiredArgsConstructor
    public Filter(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

}