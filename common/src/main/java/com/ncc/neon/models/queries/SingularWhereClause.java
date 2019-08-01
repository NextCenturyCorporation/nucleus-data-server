package com.ncc.neon.models.queries;

import java.time.ZonedDateTime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.Data;

@Data
@JsonSerialize(using = SingularWhereClauseSerializer.class)
@JsonDeserialize(using = SingularWhereClauseDeserializer.class)
public class SingularWhereClause implements WhereClause {
    String lhs;
    String operator;

    Boolean rhsBoolean;
    ZonedDateTime rhsDate;
    Double rhsDouble;
    String rhsString;

    public Object getRhs() {
        if(isBoolean()) {
            return rhsBoolean;
        }
        if(isDate()) {
            return rhsDate;
        }
        if(isDouble()) {
            return rhsDouble;
        }
        if(isString()) {
            return rhsString;
        }
        return null;
    }

    private SingularWhereClause(String lhs, String operator) {
        this.lhs = lhs;
        this.operator = operator;
    }

    private SingularWhereClause(String lhs, String operator, boolean rhs) {
        this.lhs = lhs;
        this.operator = operator;
        this.rhsBoolean = Boolean.valueOf(rhs);
    }

    private SingularWhereClause(String lhs, String operator, ZonedDateTime rhs) {
        this.lhs = lhs;
        this.operator = operator;
        this.rhsDate = rhs;
    }

    private SingularWhereClause(String lhs, String operator, double rhs) {
        this.lhs = lhs;
        this.operator = operator;
        this.rhsDouble = Double.valueOf(rhs);
    }

    private SingularWhereClause(String lhs, String operator, String rhs) {
        this.lhs = lhs;
        this.operator = operator;
        this.rhsString = rhs;
    }

    public static SingularWhereClause fromBoolean(String lhs, String operator, boolean rhs) {
        return new SingularWhereClause(lhs, operator, rhs);
    }

    public static SingularWhereClause fromDate(String lhs, String operator, ZonedDateTime rhs) {
        return new SingularWhereClause(lhs, operator, rhs);
    }

    public static SingularWhereClause fromDouble(String lhs, String operator, double rhs) {
        return new SingularWhereClause(lhs, operator, rhs);
    }

    public static SingularWhereClause fromNull(String lhs, String operator) {
        return new SingularWhereClause(lhs, operator);
    }

    public static SingularWhereClause fromString(String lhs, String operator, String rhs) {
        return new SingularWhereClause(lhs, operator, rhs);
    }

    @JsonIgnore
    public boolean isBoolean() {
        return this.rhsBoolean != null;
    }

    @JsonIgnore
    public boolean isDate() {
        return this.rhsDate != null;
    }

    @JsonIgnore
    public boolean isDouble() {
        return this.rhsDouble != null;
    }

    @JsonIgnore
    public boolean isNull() {
        return this.rhsBoolean == null && this.rhsDate == null && this.rhsDouble == null && this.rhsString == null;
    }

    @JsonIgnore
    public boolean isString() {
        return this.rhsString != null;
    }
}
