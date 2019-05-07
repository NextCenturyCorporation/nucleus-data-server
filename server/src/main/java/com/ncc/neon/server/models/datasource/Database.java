package com.ncc.neon.server.models.datasource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Database {
    String name;
    String prettyName;
    Table[] tables;

    private DataStore dataStore;

    @JsonIgnore
    public DataStore getDataStore() {
        return dataStore;
    }

    public Table getTable(@NonNull String tableName) {
        for (Table table : tables) {
            if (tableName.equals(table.getName())) {
                return table;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        if (dataStore == null) {
            return name;
        }
        return dataStore.toString() + "." + name;
    }

}
