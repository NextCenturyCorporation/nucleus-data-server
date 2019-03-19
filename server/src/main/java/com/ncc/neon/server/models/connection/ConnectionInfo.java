package com.ncc.neon.server.models.connection;

import lombok.Value;

@Value
public class ConnectionInfo {
    // Datastore type like elasticsearch or sql
    String databaseType;

    // Datastore host:port
    String host;

    // TODO Add username/password
}