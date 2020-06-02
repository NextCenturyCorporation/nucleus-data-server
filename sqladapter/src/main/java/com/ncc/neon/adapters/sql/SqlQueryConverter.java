package com.ncc.neon.adapters.sql;

import com.ncc.neon.models.queries.*;
import com.ncc.neon.util.DateUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SqlQueryConverter {
    public SqlQueryConverter() {
    }

    public static String convertQuery(Query query, SqlType type) {
        try {
            return appendLimitAndOffset(
                appendOrderBy(
                    appendGroupBy(
                        appendWhere(
                            appendJoin(
                                appendSelect(new StringBuilder(), query, type),
                                query,
                                type
                            ),
                            query,
                            type
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

    private static StringBuilder appendGroupBy(StringBuilder builder, Query query) {
        if (query.getGroupByClauses().size() > 0) {
            List<String> groups = query.getGroupByClauses().stream().map(groupBy -> {
                if (groupBy instanceof GroupByOperationClause) {
                    return ((GroupByOperationClause) groupBy).getLabel();
                }
                return ((GroupByFieldClause) groupBy).getCompleteField();
            }).collect(Collectors.toList());
            return builder.append(" GROUP BY ").append(groups.stream().collect(Collectors.joining(", ")));
        }
        return builder;
    }

    private static StringBuilder appendJoin(StringBuilder builder, Query query, SqlType type) {
        System.out.println("number of joins " + query.getJoinClauses().size());
        for (JoinClause joinClause : query.getJoinClauses()) {
            String joinType = retrieveJoinType(joinClause);
            System.out.println("join clause " + joinClause.toString());
            System.out.println("join of type " + joinType);
            if (joinType.equals("FULL JOIN") && type == SqlType.MYSQL) {
                throw new UnsupportedOperationException("MySQL does not support full joins.");
            }
            else {
                builder.append(" ").append(joinType).append(" ").append(joinClause.getDatabase()).append(".")
                    .append(joinClause.getTable()).append(" ON ")
                    .append(transformWhere(joinClause.getOnClause(), type));
            }
        }
        return builder;
    }

    private static String retrieveJoinType(JoinClause joinClause) {
        if (joinClause.getType().toUpperCase().equals("CROSS")) {
            return "CROSS JOIN";
        }
        if (joinClause.getType().toUpperCase().equals("FULL")) {
            return "FULL JOIN";
        }
        if (joinClause.getType().toUpperCase().equals("INNER")) {
            return "INNER JOIN";
        }
        if (joinClause.getType().toUpperCase().equals("LEFT")) {
            return "LEFT JOIN";
        }
        if (joinClause.getType().toUpperCase().equals("RIGHT")) {
            return "RIGHT JOIN";
        }
        return "JOIN";
    }

    private static StringBuilder appendLimitAndOffset(StringBuilder builder, Query query) throws Exception {
        if (query.getLimitClause() != null) {
            builder.append(" LIMIT ").append(query.getLimitClause().getLimit());
        }
        if (query.getOffsetClause() != null && query.getOffsetClause().getOffset() > 0) {
            if (query.getLimitClause() == null) {
                throw new Exception();
            }
            builder.append(" OFFSET ").append(query.getOffsetClause().getOffset());
        }
        return builder;
    }

    private static StringBuilder appendSelect(StringBuilder builder, Query query, SqlType type) {
        Stream<String> aggregateOnTotalStream = query.getAggregateClauses().stream()
            .filter(aggregate -> aggregate instanceof AggregateByTotalCountClause).map(aggregate ->
                aggregate.getOperation().toUpperCase() + "(*) AS " + aggregate.getLabel());

        Stream<String> aggregateByFieldStream = query.getAggregateClauses().stream()
            .filter(aggregate -> aggregate instanceof AggregateByFieldClause).map(aggregate ->
                aggregate.getOperation().toUpperCase() + "(" +
                    ((AggregateByFieldClause) aggregate).getCompleteField() + ") AS " + aggregate.getLabel());

        // TODO Do we need to support the AggregateOnGroupClause?
        Stream<String> aggregateStream = Stream.concat(aggregateOnTotalStream, aggregateByFieldStream);

        Stream<String> groupByStream = query.getGroupByClauses().stream().map(groupBy -> {
            if (groupBy instanceof GroupByOperationClause) {
                String prefix = ((GroupByOperationClause) groupBy).getOperation().toUpperCase();
                String suffix = groupBy.getCompleteField() + ") AS " + ((GroupByOperationClause) groupBy).getLabel();

                if (type == SqlType.POSTGRESQL) {
                    return "EXTRACT(" + (prefix.equals("DAYOFMONTH") ? "DAY" : prefix) + " FROM " + suffix;
                }

                return prefix + "(" + suffix;
            }
            return groupBy.getCompleteField();
        });

        // Remove fields in GROUP BY functions from the SELECT fields (they were likely added by mistake).
        List<String> groupByFunctionFields = query.getGroupByClauses().stream()
            .filter(groupBy -> groupBy instanceof GroupByOperationClause)
            .map(groupBy -> ((GroupByOperationClause) groupBy).getCompleteField())
            .collect(Collectors.toList());

        Stream<String> aggregateAndGroupStream = Stream.concat(aggregateStream, groupByStream);
        List<String> fields = (query.getSelectClause().getFieldClauses().size() == 0 ? aggregateAndGroupStream :
            Stream.concat(query.getSelectClause().getFieldClauses().stream()
                .map(fieldClause -> fieldClause.getComplete())
                .filter(field -> !groupByFunctionFields.contains(field)), aggregateAndGroupStream)).distinct()
                .collect(Collectors.toList());

        return builder.append("SELECT ").append(query.isDistinct() ? "DISTINCT " : "")
            .append((fields.size() == 0 ? "*" : fields.stream().collect(Collectors.joining(", "))))
            .append(" FROM ").append(query.getSelectClause().getDatabase()).append(".")
            .append(query.getSelectClause().getTable());
    }

    private static StringBuilder appendOrderBy(StringBuilder builder, Query query) {
        if (query.getOrderByClauses().size() > 0) {
            List<String> orderBys = query.getOrderByClauses().stream().map(orderBy ->
                orderBy.getCompleteFieldOrOperation() + (orderBy.getOrder() == Order.ASCENDING ? " ASC" : " DESC")
            ).collect(Collectors.toList());
            return builder.append(" ORDER BY ").append(orderBys.stream().collect(Collectors.joining(", ")));
        }
        return builder;
    }

    private static StringBuilder appendWhere(StringBuilder builder, Query query, SqlType type) {
        if (query.getWhereClause() != null) {
            String whereString = transformWhere(query.getWhereClause(), type);
            if (whereString != null) {
                return builder.append(" WHERE ").append(whereString);
            }
        }
        return builder;
    }

    protected static void logObject(String name, Object object) {
        log.debug(name + ":  " + object.toString());
    }

    private static String preventInjection(String fromUser) {
        return fromUser.replaceAll("'", "\\\\'");
    }

    private static String transformCompoundWhere(CompoundWhereClause where, SqlType type) {
        String joinType = where instanceof AndWhereClause ? " AND " : " OR ";
        List<WhereClause> innerWheres = where instanceof AndWhereClause ? ((AndWhereClause) where).getWhereClauses() :
            ((OrWhereClause) where).getWhereClauses();
        return "(" + innerWheres.stream().map(innerWhere -> transformWhere(innerWhere, type)).filter(innerWhereString ->
            innerWhereString != null).collect(Collectors.joining(joinType)) + ")";
    }

    private static String transformFieldsWhere(FieldsWhereClause where, SqlType type) {
        String field1 = where.getLhs().getComplete();
        String field2 = where.getRhs().getComplete();

        if (Arrays.asList("contains", "not contains", "notcontains").contains(where.getOperator())) {
            boolean not = !where.getOperator().equals("contains");
            String operator = (type == SqlType.POSTGRESQL ? (not ? "!~" : "~") : ((not ? "NOT " : "") + "REGEXP"));
            return field1 + " " + operator + " " + field2;
        }

        String operator = (where.getOperator().equals("notin") ? "NOT IN" : where.getOperator().toUpperCase());

        return field1 + " " + operator + " " + field2;
    }

    private static String transformSingularWhere(SingularWhereClause where, SqlType type) {
        String field = where.getLhs().getComplete();

        if (Arrays.asList("contains", "not contains", "notcontains").contains(where.getOperator())) {
            boolean not = !where.getOperator().equals("contains");
            String operator = (type == SqlType.POSTGRESQL ? (not ? "!~" : "~") : ((not ? "NOT " : "") + "REGEXP"));
            return field + " " + operator + " '.*" + preventInjection(where.getRhsString()) + ".*'";
        }

        if (where.isNull() && Arrays.asList("=", "!=").contains(where.getOperator())) {
            return field + " IS" + (where.getOperator().equals("=") ? "" : " NOT") + " NULL";
        }

        if (where.isBoolean() && Arrays.asList("=", "!=").contains(where.getOperator())) {
            return (!(where.getOperator().equals("=") ^ where.getRhsBoolean()) ? "" : "NOT ") + field;
        }

        String operator = (where.getOperator().equals("notin") ? "NOT IN" : where.getOperator().toUpperCase());

        if (where.isDate()) {
            String dateFunction = type == SqlType.POSTGRESQL ? "TO_TIMESTAMP" : "STR_TO_DATE";
            String dateString = DateUtil.transformDateToString(where.getRhsDate());
            String dateFormat = type == SqlType.POSTGRESQL ? "YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"" : "%Y-%m-%dT%TZ";
            return field + " " + operator + " " + dateFunction + "('" + dateString + "','" + dateFormat + "')";
        }

        Object value = (where.isString() ? ("'" + preventInjection(where.getRhsString()) + "'") : where.getRhs());

        return field + " " + operator + " " + value;
    }

    private static String transformWhere(WhereClause where, SqlType type) {
        if (where instanceof SingularWhereClause) {
            return transformSingularWhere((SingularWhereClause) where, type);
        }
        if (where instanceof CompoundWhereClause) {
            return transformCompoundWhere((CompoundWhereClause) where, type);
        }
        if (where instanceof FieldsWhereClause) {
            return transformFieldsWhere((FieldsWhereClause) where, type);
        }
        return null;
    }

    public static String convertMutationQuery(MutateQuery mutateQuery) {
        List<String> fieldAndValueStrings = mutateQuery.getFieldsWithValues().entrySet().stream()
            .map(entry -> entry.getKey() + " = " + SqlQueryConverter.transformObjectToString(entry.getValue(), false))
            .collect(Collectors.toList());

        String sqlQueryString = "UPDATE " + mutateQuery.getDatabaseName() + "." + mutateQuery.getTableName() +
            " SET " + String.join(", ", fieldAndValueStrings) + " WHERE " + mutateQuery.getIdFieldName() + " = '" +
            mutateQuery.getDataId() + "'";

        return sqlQueryString;
    }

    public static String convertMutationIntoInsertQuery(MutateQuery mutateQuery) {
        String sqlQueryString = "INSERT INTO " + mutateQuery.getDatabaseName() + "." + mutateQuery.getTableName() +
                " (" + String.join(", ", mutateQuery.getFieldsWithValues().keySet()) + ") VALUES (" +
                mutateQuery.getFieldsWithValues().values().stream().map(value -> SqlQueryConverter
                        .transformObjectToString(value, false)).collect(Collectors.joining(", ")) + ")";

        return sqlQueryString;
    }

    public static String convertMutationQueryIntoDeleteQuery(MutateQuery mutateQuery) {
        String sqlQueryString = "DELETE FROM WHERE " + mutateQuery.getDatabaseName() + "." +
                mutateQuery.getTableName() + "." + mutateQuery.getIdFieldName() + " = " + mutateQuery.getDataId();

        return sqlQueryString;
    }

    private static String transformObjectToString(Object object, boolean insideJson) {
        if (object instanceof String) {
            return (insideJson ? "\"" : "'") + object + (insideJson ? "\"" : "'");
        }
        if (object instanceof List) {
            List<String> objects = ((List<Object>)object).stream().map(item ->
                SqlQueryConverter.transformObjectToString(item, true)).collect(Collectors.toList());
            return (insideJson ? "" : "'") + "[" + String.join(",", objects) + "]" + (insideJson ? "" : "'");
        }
        if (object instanceof Map) {
            List<String> objects = ((Map<String, Object>)object).entrySet().stream().map(entry -> "\"" +
                entry.getKey() + "\":" + SqlQueryConverter.transformObjectToString(entry.getValue(), true)).collect(
                    Collectors.toList());
            return (insideJson ? "" : "'") + "{" + String.join(",", objects) + "}" + (insideJson ? "" : "'");
        }
        return object.toString();
    }
}
