package com.ncc.neon.server.models.query.filter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(fluent = true)
public class FilterEvent {

    /** a string description of the type of event */
    String type;

    /** the added filter for add and replace events */
    Filter addedFilter;

    /** the removed filter for replace and remove events */
    Filter removedFilter;
}