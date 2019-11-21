package com.ncc.neon.adapters.sql;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ncc.neon.models.queries.AndWhereClause;
import com.ncc.neon.models.queries.BooleanWhereClause;
import com.ncc.neon.models.queries.GroupByFieldClause;
import com.ncc.neon.models.queries.GroupByFunctionClause;
import com.ncc.neon.models.queries.OrWhereClause;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.SingularWhereClause;
import com.ncc.neon.models.queries.SortClauseOrder;
import com.ncc.neon.models.queries.WhereClause;
import com.ncc.neon.util.DateUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlQueryConverter {
    public SqlQueryConverter() {
    }

    public static String convertQuery(Query query, SqlType type) {
        try {
            return appendLimit(
                appendSortBy(
                    appendGroupBy(
                        appendWhere(
                            appendSelect(new StringBuilder(), query, type),
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
            return null;
        }
    }

    private static StringBuilder appendGroupBy(StringBuilder builder, Query query) {
        if (query.getGroupByClauses().size() > 0) {
            List<String> groups = query.getGroupByClauses().stream().map(groupBy -> {
                if (groupBy instanceof GroupByFunctionClause) {
                    return ((GroupByFunctionClause) groupBy).getName();
                }
                return ((GroupByFieldClause) groupBy).getField();
            }).collect(Collectors.toList());
            return builder.append(" GROUP BY ").append(groups.stream().collect(Collectors.joining(", ")));
        }
        return builder;
    }

    private static StringBuilder appendLimit(StringBuilder builder, Query query) throws Exception {
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
        Stream<String> aggregateStream = query.getAggregates().stream().map(aggregate -> {
            return aggregate.getOperation().toUpperCase() + "(" + aggregate.getField() + ") AS " + aggregate.getName();
        });

        Stream<String> groupByStream = query.getGroupByClauses().stream().map(groupBy -> {
            if (groupBy instanceof GroupByFunctionClause) {
                String prefix = ((GroupByFunctionClause) groupBy).getOperation().toUpperCase();
                String suffix = ((GroupByFunctionClause) groupBy).getField() + ") AS " +
                    ((GroupByFunctionClause) groupBy).getName();

                if (type == SqlType.POSTGRESQL) {
                    return "EXTRACT(" + (prefix.equals("DAYOFMONTH") ? "DAY" : prefix) + " FROM " + suffix;
                }

                return prefix + "(" + suffix;
            }
            return ((GroupByFieldClause) groupBy).getField();
        });

        // Remove fields in GROUP BY functions from the SELECT fields (they were likely added by mistake).
        List<String> groupByFunctionFields = query.getGroupByClauses().stream().filter(groupBy ->
            groupBy instanceof GroupByFunctionClause).map(groupBy -> ((GroupByFunctionClause) groupBy).getField())
            .collect(Collectors.toList());

        Stream<String> aggregateAndGroupStream = Stream.concat(aggregateStream, groupByStream);
        List<String> fields = ((query.getFields().size() == 1 && query.getFields().get(0).equals("*")) ?
            aggregateAndGroupStream :
            Stream.concat(
                query.getFields().stream().filter(field -> !groupByFunctionFields.contains(field)),
                aggregateAndGroupStream
            )).distinct().collect(Collectors.toList());

        return builder.append("SELECT ").append(query.isDistinct() ? "DISTINCT " : "")
            .append((fields.size() == 0 ? "*" : fields.stream().collect(Collectors.joining(", "))))
            .append(" FROM ").append(query.getFilter().getDatabaseName()).append(".")
            .append(query.getFilter().getTableName());
    }

    private static StringBuilder appendSortBy(StringBuilder builder, Query query) {
        if (query.getSortClauses().size() > 0) {
            List<String> sorts = query.getSortClauses().stream().map(sortBy -> {
                return sortBy.getFieldName() + (sortBy.getSortOrder() == SortClauseOrder.ASCENDING ? " ASC" : " DESC");
            }).collect(Collectors.toList());
            return builder.append(" ORDER BY ").append(sorts.stream().collect(Collectors.joining(", ")));
        }
        return builder;
    }

    private static StringBuilder appendWhere(StringBuilder builder, Query query, SqlType type) {
        if (query.getFilter().getWhereClause() != null) {
            String whereString = transformWhere(query.getFilter().getWhereClause(), type);
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

    private static String transformCompoundWhere(BooleanWhereClause where, SqlType type) {
        String joinType = where instanceof AndWhereClause ? " AND " : " OR ";
        List<WhereClause> innerWheres = where instanceof AndWhereClause ? ((AndWhereClause) where).getWhereClauses() :
            ((OrWhereClause) where).getWhereClauses();
        return "(" + innerWheres.stream().map(innerWhere -> transformWhere(innerWhere, type)).filter(innerWhereString ->
            innerWhereString != null).collect(Collectors.joining(joinType)) + ")";
    }

    private static String transformSingularWhere(SingularWhereClause where, SqlType type) {
        if(Arrays.asList("contains", "not contains", "notcontains").contains(where.getOperator())) {
            boolean not = !where.getOperator().equals("contains");
            String operator = (type == SqlType.POSTGRESQL ? (not ? "!~" : "~") : ((not ? "NOT " : "") + "REGEXP"));
            return where.getLhs() + " " + operator + " '.*" + preventInjection(where.getRhsString()) + ".*'";
        }

        String field = where.getLhs();

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
        if (where instanceof BooleanWhereClause) {
            return transformCompoundWhere((BooleanWhereClause) where, type);
        }
        return null;
    }
}
