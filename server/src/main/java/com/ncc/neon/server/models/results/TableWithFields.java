package com.ncc.neon.server.models.results;

import lombok.Value;
import java.util.List;

@Value
public class TableWithFields {
    String tableName;
    List<String> fields;
}
