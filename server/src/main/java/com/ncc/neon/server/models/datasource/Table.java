package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Table {
    String name;
    String prettyName;
    Field[] fields;
    // TODO - Mappings?
}
