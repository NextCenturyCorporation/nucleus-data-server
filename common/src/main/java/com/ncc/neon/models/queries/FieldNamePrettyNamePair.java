package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class FieldNamePrettyNamePair {
    private String query;
    private String pretty;
}