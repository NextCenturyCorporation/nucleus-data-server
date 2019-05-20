package com.ncc.neon.server.models.results;

import java.util.Map;

import lombok.Getter;
import lombok.Value;

@Value
public class PagedList<T extends Map> {
  T[] results;
  
  int total;

  public PagedList(T[] results, int total) {
    this.results = results;
    this.total = total;
  }

  public T[] getResults() {
    return this.results;
  }

  public int getTotal() {
    return this.total;
  }
}