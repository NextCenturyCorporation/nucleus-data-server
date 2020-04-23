package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Data
public class ClusterClause {
    int count;
    String type;
    List<List<Object>> clusters;
    String aggregationName;
    String fieldType;
    List<String> fieldNames;
}
