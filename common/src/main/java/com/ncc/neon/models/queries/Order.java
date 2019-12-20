package com.ncc.neon.models.queries;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@JsonDeserialize(using = OrderDeserializer.class)
public enum Order {

    ASCENDING(1), DESCENDING(-1);

    @JsonValue
    @Getter
    private final int direction;

    public static Order fromDirection(int direction) {
        return Arrays.stream(Order.values()).filter(sortOrder -> sortOrder.direction == direction).findAny().get();
    }
}
