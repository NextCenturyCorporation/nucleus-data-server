package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class ImportQuery {
    String hostName;
    String dataStoreType;
    String database;
    String table;
    List<String> source;
    boolean isNew;
}