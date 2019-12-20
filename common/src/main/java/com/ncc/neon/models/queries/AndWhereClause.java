package com.ncc.neon.models.queries;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class AndWhereClause extends CompoundWhereClause {
    public AndWhereClause(List<WhereClause> whereClauses) {
        super(whereClauses);
    }
}
