package com.ncc.neon.server.models.datasource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Field {
    String columnName;
    String prettyName;
    String type;

    private Table table;

    @JsonIgnore
    public Table getTable() {
        return table;
    }

    @Override
    public String toString() {
        if (table == null) {
            return columnName;
        }
        return table.toString() + "." + columnName;
    }
}
