package com.ncc.neon.models.results;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum FieldType {
    BOOLEAN ("boolean"),
    DATETIME ("date"),
    DECIMAL ("decimal"),
    GEO ("geo"),
    ID ("id"),
    INTEGER ("integer"),
    KEYWORD ("keyword"),
    OBJECT ("object"),
    TEXT ("text");

    public String name;

    public String toString() {
        return this.name;
    }
}