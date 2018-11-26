package com.ncc.neon.server.models.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * TabularQueryResult
 */
public class TabularQueryResult implements QueryResult<List<Map<String, Object>>> {
    final List<Map<String, Object>> data;

    TabularQueryResult() {
        this(Collections.emptyList());
    }

    TabularQueryResult(List<Map<String, Object>> table) {
        this.data = table;
    }

    @Override
    public List<Map<String, Object>> getData() {
        return data;
    }
}