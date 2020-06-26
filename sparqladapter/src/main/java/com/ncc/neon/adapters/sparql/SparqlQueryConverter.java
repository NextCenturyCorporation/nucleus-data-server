package com.ncc.neon.adapters.sparql;

import com.ncc.neon.models.queries.*;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SparqlQueryConverter {
    public SparqlQueryConverter() {
    }

    public static String convertQuery(Query query) {
        try {
            return appendLimitAndOffset(
                appendOrderBy(
                    appendGroupBy(
                        appendWhere(
                            appendSelect(new StringBuilder(), query),
                                query
                                ),
                        query
                        ),
                    query
                    ),
                query
            ).toString();
        } catch (Exception e) {
            System.err.println(e);
            return null;
        }
    }

    private static StringBuilder appendLimitAndOffset(StringBuilder builder, Query query) throws Exception {
        if (query.getLimitClause() != null) {
            builder.append(" LIMIT ").append(query.getLimitClause().getLimit());
        }
        if (query.getOffsetClause() != null) {
            builder.append(" OFFSET ").append(query.getOffsetClause().getOffset());
        }
        return builder;
    }

    private static StringBuilder appendOrderBy(StringBuilder builder, Query query) {
        if (query.getOrderByClauses().size() > 0) {
            List<String> orderBys = query.getOrderByClauses().stream().map(orderBy ->
                    (orderBy.getOrder() == Order.ASCENDING ? " ASC" : " DESC") + "(?" + orderBy.getFieldOrOperation() + ")"
            ).collect(Collectors.toList());
            return builder.append(" ORDER BY").append(orderBys.stream().collect(Collectors.joining(" ")));
        }
        return builder;
    }

    private static StringBuilder appendGroupBy(StringBuilder builder, Query query) {
        if (query.getGroupByClauses().size() > 0) {
            List<String> groups = query.getGroupByClauses().stream().map(groupBy -> {
                if (groupBy instanceof GroupByOperationClause) {
                    return ((GroupByOperationClause) groupBy).getLabel();
                }
                return ((GroupByFieldClause) groupBy).getField();
            }).collect(Collectors.toList());
            return builder.append(" GROUP BY ").append(groups.stream().map(group -> "?" + group)
                    .collect(Collectors.joining(" ")));
        }
        return builder;
    }

    private static StringBuilder appendWhere(StringBuilder builder, Query query) {
        if (query.getWhereClause() != null) {
            String whereString = transformWhere(query.getWhereClause());
            if (whereString != null) {
                return builder.append(" WHERE { ").append(whereString).append("} ");
            }
        } else {
            // default -- query doesn't work without this
            builder.append(" WHERE { ?s ?p ?o }");
        }
        return builder;
    }

    private static String transformWhere(WhereClause where) {
        if (where instanceof SingularWhereClause) {
            return transformSingularWhere((SingularWhereClause) where);
        }
        if (where instanceof CompoundWhereClause) {
            return transformCompoundWhere((CompoundWhereClause) where);
        }
        return null;
    }

    private static String transformSingularWhere(SingularWhereClause where) {
        String subject = "?" + where.getLhs().getField();
        String predicate = "?" + where.getOperator();
        String object = "?" + where.getRhs().toString();

        return subject + " " + predicate + " " + object;
    }

    private static String transformCompoundWhere(CompoundWhereClause where) {
        String joinType = where instanceof AndWhereClause ? " \n " : " OR ";
        List<WhereClause> innerWheres = where instanceof AndWhereClause ? ((AndWhereClause) where).getWhereClauses() :
                ((OrWhereClause) where).getWhereClauses();
        return innerWheres.stream().map(innerWhere -> transformWhere(innerWhere)).filter(innerWhereString ->
                innerWhereString != null).collect(Collectors.joining(joinType));
    }

    private static StringBuilder appendSelect(StringBuilder builder, Query query) {
        List<String> fields = query.getSelectClause().getFieldClauses().stream().map(field -> field.getField())
                .collect(Collectors.toList());
        return builder.append("SELECT ").append(query.isDistinct() ? "DISTINCT " : "")
                .append((fields.size() == 0 ? "*" : fields.stream().map(field -> "?" + field)
                        .collect(Collectors.joining(" "))));

    }

    public static String convertMutationInsertQuery(MutateQuery mutateQuery) {
        //TODO: maybe use field values instead?
        return "INSERT DATA { " + mutateQuery.getDatabaseName() + " " + mutateQuery.getTableName() + " \""
            + mutateQuery.getDataId() + "\" }";
    }

    public static String convertMutationDeleteQuery(MutateQuery mutateQuery) {
        return "DELETE DATA { " + mutateQuery.getDatabaseName() + " " + mutateQuery.getTableName() + " "
                + mutateQuery.getDataId() + " }";
    }
}
