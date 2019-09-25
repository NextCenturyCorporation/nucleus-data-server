package com.ncc.neon.models.queries;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class ExportQuery {

    String fileName;
    String databaseType;
    String hostName;
    Query query;
    Map<String, String> queryFieldNameMap;    //mapping of field names to pretty field names

}