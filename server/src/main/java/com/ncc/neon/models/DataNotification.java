package com.ncc.neon.models;

import lombok.Data;
import lombok.Value;

@Data
public class DataNotification {
  long count;
  long timestamp;
  long publishDate;
}
