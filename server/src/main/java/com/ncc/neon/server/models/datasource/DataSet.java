package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DataSet {
    String name;
    String datastore;
    String hostname;
    boolean connectOnLoad;
    Database[] databases;
}
