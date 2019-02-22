package com.ncc.neon.server.models.query.clauses;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class SortOrderDeserializer extends StdDeserializer<SortOrder> {

    private static final long serialVersionUID = 1428331383389586619L;

    public SortOrderDeserializer() {
        this(null);
    }

    public SortOrderDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public SortOrder deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        return SortOrder.fromDirection(p.getIntValue());
    }
}

