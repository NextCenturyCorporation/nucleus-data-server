package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Database {
    String name;
    String prettyName;
    Table[] tables;
}
