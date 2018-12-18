package com.ncc.neon.server.models.query.clauses;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * SortOrderDeserializer
 */
public class SortOrderDeserializer extends StdDeserializer<SortOrder> {

    public SortOrderDeserializer() {
        this(null);
    }

    public SortOrderDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public SortOrder deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException, JsonProcessingException {

        return SortOrder.fromDirection(p.getIntValue());
    }
}