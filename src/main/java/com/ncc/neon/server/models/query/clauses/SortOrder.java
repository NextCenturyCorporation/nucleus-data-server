package com.ncc.neon.server.models.query.clauses;

import java.util.Arrays;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * SortOrder
 */
@Accessors(fluent = true)
@JsonDeserialize(using = SortOrderDeserializer.class)
public enum SortOrder {
    ASCENDING(1), DESCENDING(-1);

    @Getter
    private final int direction;

    SortOrder(int direction) {
        this.direction = direction;
    }

    static SortOrder fromDirection(int direction) {
        // TODO:maybe right
        return Arrays.stream(SortOrder.values()).filter(so -> so.direction == direction).findAny().get();
    }

}
