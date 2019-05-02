package com.ncc.neon.server.models.datasource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Table {
    String name;
    String prettyName;
    Field[] fields;

    private Database database;

    @JsonIgnore
    public Database getDatabase() {
        return this.database;
    }

    public Field getField(@NonNull String name) {
        for (Field field : fields) {
            if (name.equals(field.getColumnName())) {
                return field;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        if (database == null) {
            return name;
        }
        return database.toString() + "." + name;
    }
}
