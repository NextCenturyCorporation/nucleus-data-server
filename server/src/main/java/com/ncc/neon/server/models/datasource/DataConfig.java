package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DataConfig {
    DataStore[] dataStores;

    Map<String, Map<String, String[]>> dataSets;
}
