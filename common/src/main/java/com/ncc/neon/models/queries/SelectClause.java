package com.ncc.neon.models.queries;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class SelectClause {
    String database;
    String table;
    List<FieldClause> fieldClauses = new ArrayList<>();

    public SelectClause(String databaseName, String tableName) {
        this(databaseName, tableName, new ArrayList<>());
    }
}
