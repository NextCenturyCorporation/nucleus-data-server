package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class FieldClause {
    String database;
    String table;
    String field;

    public String getComplete() {
        return database + "." + table + "." + field;
    }
}
