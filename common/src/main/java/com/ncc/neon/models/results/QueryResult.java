package com.ncc.neon.models.results;

import java.util.Map;

public interface QueryResult<T> {
   T getData();
   Map<String, Object> getFirstOrNull();
}
