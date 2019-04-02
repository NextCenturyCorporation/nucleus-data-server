package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Field {
    String columnName;
    String prettyName;
    Type type;

    // TODO - Determine possible types, or store type as a string
    public enum Type {
        DOUBLE,
        INTEGER,
        STRING,
        DATETIME
    }
}
