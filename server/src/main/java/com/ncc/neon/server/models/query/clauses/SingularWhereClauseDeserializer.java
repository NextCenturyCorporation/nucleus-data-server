package com.ncc.neon.server.models.query.clauses;

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class SingularWhereClauseDeserializer extends StdDeserializer<SingularWhereClause> {
    private static final long serialVersionUID = 7819782490334351351L;

    private static final DateTimeFormatter DATE_PARSER = ISODateTimeFormat.dateTimeParser().withZoneUTC();

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
            try {
                // TODO Add non-ISO date parsers
                Date date = DATE_PARSER.parseDateTime(rhs.asText()).toDate();
                return SingularWhereClause.fromDate(node.get("lhs").asText(), node.get("operator").asText(), date);
            }
            catch(Exception e) {
                return SingularWhereClause.fromString(node.get("lhs").asText(), node.get("operator").asText(), rhs.asText());
            }
        }
        return SingularWhereClause.fromNull(node.get("lhs").asText(), node.get("operator").asText());
    }
}