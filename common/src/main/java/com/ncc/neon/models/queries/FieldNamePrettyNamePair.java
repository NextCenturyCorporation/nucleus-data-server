package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class FieldNamePrettyNamePair {
    public String query;
    public String pretty;
}