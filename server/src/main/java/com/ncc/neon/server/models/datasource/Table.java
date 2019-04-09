package com.ncc.neon.server.models.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;

@AllArgsConstructor
@Data
public class Table {
    String name;
    String prettyName;
    Field[] fields;
    HashMap<String, String> mappings;
}
