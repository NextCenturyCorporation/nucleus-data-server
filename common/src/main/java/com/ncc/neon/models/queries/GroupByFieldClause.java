package com.ncc.neon.models.queries;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class GroupByFieldClause implements GroupByClause {
    String field;
    String prettyField;
}
