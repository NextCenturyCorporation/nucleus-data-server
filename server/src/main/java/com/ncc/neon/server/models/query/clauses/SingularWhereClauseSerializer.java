package com.ncc.neon.server.models.query.clauses;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class SingularWhereClauseSerializer extends StdSerializer<SingularWhereClause> {
    private static final long serialVersionUID = -3365710011423873638L;

    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.dateTimeNoMillis();

    public SingularWhereClauseSerializer() {
        this(null);
    }

    public SingularWhereClauseSerializer(Class<SingularWhereClause> t) {
        super(t);
    }

    @Override
    public void serialize(SingularWhereClause clause, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        gen.writeStringField("lhs", clause.getLhs());
        gen.writeStringField("operator", clause.getOperator());
        if(clause.isBoolean()) {
            gen.writeBooleanField("rhs", clause.getRhsBoolean());
        }
        else if(clause.isDate()) {
            DateTime date = new DateTime(clause.getRhsDate());
            gen.writeStringField("rhs", DATE_FORMATTER.print(date.withZone(DateTimeZone.UTC)));
        }
        else if(clause.isDouble()) {
            gen.writeNumberField("rhs", clause.getRhsDouble());
        }
        else if(clause.isString()) {
            gen.writeStringField("rhs", clause.getRhsString());
        }
        else {
            gen.writeNullField("rhs");
        }
    }

    @Override
    public void serializeWithType(SingularWhereClause clause, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
    throws IOException, JsonProcessingException {
        typeSer.writeTypePrefix(gen, typeSer.typeId(clause, JsonToken.START_OBJECT));
        serialize(clause, gen, serializers);
        typeSer.writeTypeSuffix(gen, typeSer.typeId(clause, JsonToken.START_OBJECT));
    }
}