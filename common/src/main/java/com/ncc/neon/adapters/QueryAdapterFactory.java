package com.ncc.neon.adapters;

import com.ncc.neon.models.ConnectionInfo;

public interface QueryAdapterFactory {

    /**
     * Gets the name of the fiendly name of the adapter.
     */
    String getName();

    /**
     * Responsable to for connecting to the datastore
     * 
     * @param cInfo {@link ConnectionInfo} used to connect to the data store
     */
    QueryAdapter initialize(ConnectionInfo cInfo);
}
