package com.ncc.neon.models.queries;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class MutateQuery {
    String datastoreHost;
    String datastoreType;
    String databaseName;
    String tableName;
    String idFieldName;
    String dataId;
    Map<String, Object> fieldsWithValues;
}
