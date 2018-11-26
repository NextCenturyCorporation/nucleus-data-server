package com.ncc.neon.server.models.query;

import java.util.List;
import java.util.Map;

/**
 * ListQueryResult
 */
public class ListQueryResult<T> implements QueryResult<List<T>> {
    
    final List<T> data;

    ListQueryResult() {
        this();
    }

    ListQueryResult(List<T> list){
        this.data = list;
    }

    @Override
    public List<T> getData() {
        return data;
    }

    
    
    
}