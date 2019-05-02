package com.ncc.neon.server.models.datasource;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Slf4j
public class DataConfig {
    DataStore[] dataStores;

    Map<String, Map<String, String[]>> dataSets;

    //@Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, List<Table>> dataSetMap;

    //@Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, DataStore> dataStoreMap;

    public DataStore getDataStore(String name) {
        if (dataStoreMap == null) {
            buildDataStoreMap();
        }
        return this.dataStoreMap.get(name);
    }

    public List<String> getDatabaseNames(String dataSetName) {
        List<Table> tables = getTables(dataSetName);
        if (tables == null){
            return null;
        }

        Set<String> dbNames = new HashSet<>();

        for (Table table : tables) {
            dbNames.add(table.getDatabase().toString());
        }

        return new ArrayList<>(dbNames);
    }

    public List<String> getTableNames(String dataSetName) {
        List<Table> tables = getTables(dataSetName);
        if (tables == null) {
            return null;
        }

        List<String> tableNames = new ArrayList<>();
        for (Table table : tables) {
            tableNames.add(table.toString());
        }

        return tableNames;
    }

    public List<Table> getTables(String dataSetName) {
        if (dataSetMap == null) {
            buildDataSetMap();
        }

        return dataSetMap.get(dataSetName);
    }

    public void build() {
        buildDataStoreMap();
        buildDataSetMap();
    }

    private void buildDataStoreMap() {
        dataStoreMap = new HashMap<>();
        for (DataStore dataStore : dataStores) {
            dataStoreMap.put(dataStore.getName(), dataStore);
        }
    }

    private void buildDataSetMap() {
        for (DataStore dataStore : dataStores) {
            dataStore.build();
        }

        dataSetMap = new HashMap<>();
        for (String dataSetName : dataSets.keySet()) {
            Map<String, String[]> dataSetInfo = dataSets.get(dataSetName);

            List<Table> dataSetTables = new ArrayList<>();

            for (String dataStoreDbName : dataSetInfo.keySet()) {
                Database db = null;
                String[] names = dataStoreDbName.split("\\.");
                if (names.length == 2) {
                    db = getDataStore(names[0]).getDatabase(names[1]);
                }
                if (db == null) {
                    log.warn("Could not get data store and database name from " + dataStoreDbName);
                    continue;
                }

                String[] tables = dataSetInfo.get(dataStoreDbName);
                for (String tableName : tables) {
                    Table table = db.getTable(tableName);
                    if (table != null) {
                        dataSetTables.add(table);
                    }
                }

                dataSetMap.put(dataSetName, dataSetTables);
            }

        }
    }
}
