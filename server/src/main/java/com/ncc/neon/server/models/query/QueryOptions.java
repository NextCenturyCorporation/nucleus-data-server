package com.ncc.neon.server.models.query;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryOptions {
    boolean ignoreFilters;
    boolean selectionOnly;
    String refinementSpecifier;
    Set<String> ignoredFilterIds;
}
