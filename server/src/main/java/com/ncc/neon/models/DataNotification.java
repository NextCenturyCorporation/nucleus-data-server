package com.ncc.neon.models;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DataNotification {
  long count;
  String databaseName;
  String datastoreHost;
  String datastoreType;
  String tableName;
  String timestamp;

  public DataNotification(long count) {
    this.count = count;
  }

  public DataNotification(String datastoreHost, String datastoreType, String databaseName, String tableName, long count) {
    this.datastoreHost = datastoreHost;
    this.datastoreType = datastoreType;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.count = count;
  }
}
