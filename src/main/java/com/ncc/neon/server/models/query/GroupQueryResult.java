package com.ncc.neon.server.models.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * GroupQueryResult
 */
public class GroupQueryResult implements QueryResult<List<List<Map<String, Object>>>> {
    final List<List<Map<String, Object>>> data;

    GroupQueryResult() {
        this(Collections.emptyList());
    }

    GroupQueryResult(List<List<Map<String, Object>>> table) {
        this.data = table;
    }

    @Override
    public List<List<Map<String, Object>>> getData() {
        return this.data;
    }

}