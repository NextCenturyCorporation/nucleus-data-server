package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

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
    WhereClause whereClause;
}
