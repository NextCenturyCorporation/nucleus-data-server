package com.ncc.neon.models.results;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public interface QueryResult<T> {
   T getData();
   @JsonIgnore
   Map<String, Object> getFirstOrNull();
}
