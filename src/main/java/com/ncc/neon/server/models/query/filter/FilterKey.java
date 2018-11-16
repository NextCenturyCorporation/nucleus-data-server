package com.ncc.neon.server.models.query.filter;

import java.io.Serializable;

/**
 * Stores a filter with its id
 */
public class FilterKey implements Serializable {
    private static final long serialVersionUID = -5783657018410727352L;
    String id;
    Filter filter;

    // TODO: fix
    /*
     * DataSet getDataSet() { return new DataSet(databaseName: filter.databaseName,
     * tableName: filter.tableName) }
     */

}
