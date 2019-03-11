package com.ncc.neon.server.models.query.result;

import lombok.Value;
import java.util.List;

@Value
public class TableWithFields {
    String tableName;
    List<String> fields;
}