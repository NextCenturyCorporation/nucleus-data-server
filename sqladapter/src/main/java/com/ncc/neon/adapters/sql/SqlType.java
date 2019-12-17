package com.ncc.neon.adapters.sql;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum SqlType {
    MYSQL ("MySQL", "r2dbc:mysql", "mysql"),
    POSTGRESQL ("PostgreSQL", "r2dbc:postgres", "postgresql");

    public String prettyName;
    public String driverName;
    public String configName;

    public String toString() {
        return this.prettyName;
    }
}