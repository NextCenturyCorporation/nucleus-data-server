package com.ncc.neon.server.models.queries;

import java.io.IOException;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
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
        JsonNode rhs = node.get("rhs");
        if(rhs.isBoolean()) {
            return SingularWhereClause.fromBoolean(node.get("lhs").asText(), node.get("operator").asText(),
                ((BooleanNode) rhs).booleanValue());
        }
        if(rhs.isNumber()) {
            return SingularWhereClause.fromDouble(node.get("lhs").asText(), node.get("operator").asText(),
                ((DoubleNode) rhs).doubleValue());
        }
        if(rhs.isTextual()) {
            ZonedDateTime date = DateUtil.transformStringToDate(rhs.asText());

            if(date != null) {
                return SingularWhereClause.fromDate(node.get("lhs").asText(), node.get("operator").asText(), date);
            } else {
                return SingularWhereClause.fromString(node.get("lhs").asText(), node.get("operator").asText(), rhs.asText());
            }
        }
        return SingularWhereClause.fromNull(node.get("lhs").asText(), node.get("operator").asText());
    }
}

