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

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, List<Table>> dataSetMap;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, DataStore> dataStoreMap;

    public DataStore getDataStore(String name) {
        if (dataStoreMap == null) {
            build();
        }
        return this.dataStoreMap.get(name);
    }

    /**
     * Get the database from the &lt;DataStore&gt;.&lt;Database&gt; path string
     * @param path the "path" to the database in the above format
     */
    public Database getDatabase(@NonNull String path) {
        String[] parts = path.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("The path should be in <DataStore>.<Database> format");
        }
        DataStore dataStore = getDataStore(parts[0]);
        if (dataStore == null) {
            return null;
        }
        return dataStore.getDatabase(parts[1]);
    }

    /**
     * Get the table from the &lt;DataStore&gt;.&lt;Database&gt;.&lt;Table&gt; path string
     * @param path the "path" to the table in the above format
     */
    public Table getTable(@NonNull  String path) {
        String[] parts = path.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("The path should be in <DataStore>.<Database>.<Table> format");
        }
        Database database = getDatabase(String.join(".", parts[0], parts[1]));
        if (database == null) {
            return null;
        }
        return database.getTable(parts[2]);
    }

    /**
     * Get the table from the &lt;DataStore&gt;.&lt;Database&gt;.&lt;Table&gt;.&lt;Field&gt; path string
     * @param path the "path" to the field in the above format
     */
    public Field getField(@NonNull String path) {
        String[] parts = path.split("\\.");
        if (parts.length != 4) {
            throw new IllegalArgumentException("The path should be in <DataStore>.<Database>.<Table>.<Field> format");
        }
        Table table = getTable(String.join(".", parts[0], parts[1], parts[2]));
        if (table == null) {
            return null;
        }
        return table.getField(parts[3]);
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
            build();
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
