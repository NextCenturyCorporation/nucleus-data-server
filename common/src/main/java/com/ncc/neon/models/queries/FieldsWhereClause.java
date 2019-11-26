package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class FieldsWhereClause implements WhereClause {
    FieldClause lhs;
    String operator;
    FieldClause rhs;
}
