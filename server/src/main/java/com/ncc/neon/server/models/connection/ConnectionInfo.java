package com.ncc.neon.server.models.connection;

import lombok.Value;

/**
 * ConnectionInfo
 */
@Value
public class ConnectionInfo {

    /** The type of database being connected to */
    // DataSources dataSource
    String databaseType;

    /** The database host, and optionally :port */
    String host;


    //Add port, username, pass in the future.
}