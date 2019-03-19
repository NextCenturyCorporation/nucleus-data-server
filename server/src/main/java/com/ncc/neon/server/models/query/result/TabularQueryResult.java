package com.ncc.neon.server.models.query.result;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.Data;

/**
 * TabularQueryResult
 */
@Data
public class TabularQueryResult implements QueryResult<List<Map<String, Object>>> {
    final List<Map<String, Object>> data;

   public TabularQueryResult() {
        this(Collections.emptyList());
    }

    public TabularQueryResult(List<Map<String, Object>> table) {
        this.data = table;
    }

    @Override
    public List<Map<String, Object>> getData() {
        return data;
    }
}