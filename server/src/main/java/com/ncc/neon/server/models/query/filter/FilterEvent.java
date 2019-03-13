package com.ncc.neon.server.models.query.filter;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(fluent = true)
@JsonAutoDetect(getterVisibility=Visibility.NONE)
// Added @JsonAutoDetect line because of issue with filterservice/clearfilters endpoint:
// https://stackoverflow.com/questions/28466207/could-not-find-acceptable-representation-using-spring-boot-starter-web/28466881#28466881
public class FilterEvent {

    /** a string description of the type of event */
    String type;

    /** the added filter for add and replace events */
    Filter addedFilter;

    /** the removed filter for replace and remove events */
    Filter removedFilter;
}