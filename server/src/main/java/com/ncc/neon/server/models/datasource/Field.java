package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Field {
    String columnName;
    String prettyName;
    String type;
}
