package com.ncc.neon.models.queries;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class OrderDeserializer extends StdDeserializer<Order> {

    private static final long serialVersionUID = 1428331383389586619L;

    public OrderDeserializer() {
        this(null);
    }

    public OrderDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Order deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        return Order.fromDirection(p.getIntValue());
    }
}

