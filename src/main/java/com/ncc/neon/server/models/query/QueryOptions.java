package com.ncc.neon.server.models.query;

import java.util.HashSet;
import java.util.Set;

import lombok.Data;
import lombok.Value;

/**
 * QueryOptions
 */
@Value
public class QueryOptions {


     /** default options that applies filters to the query */
     //TODO: remove?? this is only used for tests...
     public static final QueryOptions DEFAULT_OPTIONS = new QueryOptions(false, false, null, new HashSet<>());

    boolean ignoreFilters;

     // TODO: Figure out what this means? does it mean that we are not doing
     // aggregations????
     boolean selectionOnly;

     // Used to refine a query.
     // Currently only used in a point_layer in the GTD's map, to limit
     // elasticsearch's query to a visible region.
     String refinementSpecifier;

     /**
      * ignores these particular filters only (ignoreFilters takes precedence). this
      * is useful if a visualization wants to ignore its own filters
      */
     Set<String> ignoredFilterIds;
}