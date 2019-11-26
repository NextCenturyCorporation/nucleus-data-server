package com.ncc.neon.models.queries;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class CompoundWhereClause implements WhereClause {
    List<WhereClause> whereClauses;
}
