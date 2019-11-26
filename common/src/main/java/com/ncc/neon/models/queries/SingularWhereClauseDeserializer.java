package com.ncc.neon.models.queries;

import java.io.IOException;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.ncc.neon.util.DateUtil;

public class SingularWhereClauseDeserializer extends StdDeserializer<SingularWhereClause> {
    private static final long serialVersionUID = 7819782490334351351L;

    public SingularWhereClauseDeserializer() {
        this(null);
    }

    public SingularWhereClauseDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public SingularWhereClause deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        JsonNode lhs = node.get("lhs");
        JsonNode rhs = node.get("rhs");
        String database = lhs.get("database").asText();
        String table = lhs.get("table").asText();
        String field = lhs.get("field").asText();
        FieldClause fieldClause = new FieldClause(database, table, field);
        String operator = node.get("operator").asText();
        if(rhs.isBoolean()) {
            return SingularWhereClause.fromBoolean(fieldClause, operator,
                ((BooleanNode) rhs).booleanValue());
        }
        if(rhs.isDouble()) {
            return SingularWhereClause.fromDouble(fieldClause, operator,
                ((DoubleNode) rhs).doubleValue());
        }
        if(rhs.isInt()) {
            return SingularWhereClause.fromDouble(fieldClause, operator,
                ((IntNode) rhs).doubleValue());
        }
        if(rhs.isTextual()) {
            ZonedDateTime date = DateUtil.transformStringToDate(rhs.asText());

            if(date != null) {
                return SingularWhereClause.fromDate(fieldClause, operator, date);
            } else {
                return SingularWhereClause.fromString(fieldClause, operator, rhs.asText());
            }
        }
        return SingularWhereClause.fromNull(fieldClause, operator);
    }
}

