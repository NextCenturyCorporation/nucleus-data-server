package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class UploadQuery {

    String fileName;
    String dataStoreType;
    String hostName;
    Query query;
    List<FieldNamePrettyNamePair> fieldNamePrettyNamePairs;
}