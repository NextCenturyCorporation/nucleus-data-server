package com.ncc.neon.server.models.results;

import java.util.Map;

import lombok.Data;

@Data
public class PagedList<T extends Map> {
  
  T[] results;
  
  int total;

  public PagedList(T[] results, int total) {
    this.results = results;
    this.total = total;
  }
}