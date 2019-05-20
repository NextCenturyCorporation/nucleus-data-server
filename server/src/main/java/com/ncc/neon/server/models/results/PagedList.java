package com.ncc.neon.server.models.results;

import lombok.Value;

@Value
public class PagedList<T> {
  T[] results;
  int total;

  public PagedList(T[] results, int total) {
    this.results = results;
    this.total = total;
  }
}