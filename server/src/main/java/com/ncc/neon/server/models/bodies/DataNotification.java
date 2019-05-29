package com.ncc.neon.server.models.bodies;

import lombok.Data;
import lombok.Value;

@Data
public class DataNotification {
  long count;
  long timestamp;
  long publishDate;
}