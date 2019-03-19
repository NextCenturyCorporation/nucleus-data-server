package com.ncc.neon.server.models.queries;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@JsonDeserialize(using = SortOrderDeserializer.class)
public enum SortOrder {
    
    ASCENDING(1), DESCENDING(-1);

    @JsonValue
    @Getter
    private final int direction;

    public static SortOrder fromDirection(int direction) {
        return Arrays.stream(SortOrder.values()).filter(sortOrder -> sortOrder.direction == direction).findAny().get();
    }
}
