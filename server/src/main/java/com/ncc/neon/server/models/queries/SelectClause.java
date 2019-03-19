package com.ncc.neon.server.models.queries;

import java.util.Arrays;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class SelectClause {
    public static final List<String> ALL_FIELDS = Arrays.asList("*");

    String databaseName;
    String tableName;

    private List<String> fields = ALL_FIELDS;

    boolean isSelectAllFields() {
        return fields == ALL_FIELDS;
    }
}
