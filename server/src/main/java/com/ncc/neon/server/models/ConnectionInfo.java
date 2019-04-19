package com.ncc.neon.server.models;

import lombok.Value;

@Value
public class ConnectionInfo {
    // DataStore type like elasticsearch or sql
    String databaseType;

    // DataStore host:port
    String host;

    // TODO Add username/password
}
