package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class DataSet {
    String name;
    String datastore;
    String hostname;
    Database[] databases;
}
