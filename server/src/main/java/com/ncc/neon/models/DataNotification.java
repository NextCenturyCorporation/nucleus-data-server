package com.ncc.neon.models;

import lombok.Data;

@Data
public class DataNotification {
  long count;
  String databaseName;
  String datastoreHost;
  String datastoreType;
  String tableName;
  String timestamp;
}
