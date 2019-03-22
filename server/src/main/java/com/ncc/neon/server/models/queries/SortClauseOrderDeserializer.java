package com.ncc.neon.server.models.queries;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class SortClauseOrderDeserializer extends StdDeserializer<SortClauseOrder> {

    private static final long serialVersionUID = 1428331383389586619L;

    public SortClauseOrderDeserializer() {
        this(null);
    }

    public SortClauseOrderDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public SortClauseOrder deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        return SortClauseOrder.fromDirection(p.getIntValue());
    }
}

