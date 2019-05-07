package com.ncc.neon.server.models.datasource;

import lombok.*;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class DataStore {
    String name;
    String hostname;
    String type;
    Database[] databases;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Map<String, Database> databaseMap;

    public Database getDatabase(@NonNull String name) {
        if (databaseMap == null) {
            databaseMap = new HashMap<>();
            for (Database database : databases) {
                databaseMap.put(database.getName(), database);
            }
        }

        return databaseMap.get(name);
    }

    public void build() {
        for (Database database : databases) {
            database.setDataStore(this);
            for (Table table : database.getTables()) {
                table.setDatabase(database);
                for (Field field : table.getFields()) {
                    field.setTable(table);
                }
            }
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
