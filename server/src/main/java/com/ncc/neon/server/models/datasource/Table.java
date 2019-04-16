package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Table {
    String name;
    String prettyName;
    Field[] fields;
    HashMap<String, String> mappings;
}
