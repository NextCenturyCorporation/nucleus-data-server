package com.ncc.neon.server.models.results;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class TabularQueryResult implements QueryResult<List<Map<String, Object>>> {
    final List<Map<String, Object>> data;

    public TabularQueryResult() {
        this(Collections.emptyList());
    }

    @Override
    public List<Map<String, Object>> getData() {
        return data;
    }
}
