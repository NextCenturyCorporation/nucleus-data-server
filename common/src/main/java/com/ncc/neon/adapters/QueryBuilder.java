package com.ncc.neon.adapters;

import com.ncc.neon.models.queries.*;
import com.ncc.neon.util.DateUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * Builds queries for the query adapters' unit tests.
 * The tests can't seem to find this file unless it's on the "src/main/java" path.
 */
public class QueryBuilder {

    protected Query buildQueryBase() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        return query;
    }

    protected Query buildQueryFields() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testField1"),
            new FieldClause("testDatabase", "testTable", "testField2")
        )));
        return query;
    }

    protected Query buildQueryDistinct() {
        Query query = new Query();
        query.setDistinct(true);
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testField1"))));
        return query;
    }

    protected Query buildQueryDistinctSparql() {
        Query query = new Query();
        query.setDistinct(true);
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
                new FieldClause("testDatabase", "testTable", "s"))));
        return query;
    }

    protected Query buildQueryFilterEqualsValueSparql() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
                new FieldClause("testDatabase", "testTable", "s"),
                new FieldClause("testDatabase", "testTable", "p"),
                new FieldClause("testDatabase", "testTable", "o"))));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "o"), "testType", "testObject"));
        return query;
    }

    protected Query buildQueryMultipleFilterEqualsValueSparql() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
                new FieldClause("testDatabase", "testTable", "s"),
                new FieldClause("testDatabase", "testTable", "p"),
                new FieldClause("testDatabase", "testTable", "o"))));
        query.setWhereClause(new AndWhereClause(Arrays.asList(
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "o"), "testObjectType", "testObject"),
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "p"), "testPredicateType", "testPredicate")
        )));
        return query;
    }

    protected Query buildQueryFilterEqualsBoolean() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromBoolean(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", true));
        return query;
    }

    protected Query buildQueryFilterEqualsDate() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDate(new FieldClause("testDatabase", "testTable", "testFilterField"), "=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z")));
        return query;
    }

    protected Query buildQueryFilterEqualsEmpty() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", ""));
        return query;
    }

    protected Query buildQueryFilterEqualsFalse() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromBoolean(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", false));
        return query;
    }

    protected Query buildQueryFilterEqualsNull() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromNull(new FieldClause("testDatabase", "testTable", "testFilterField"), "="));
        return query;
    }

    protected Query buildQueryFilterEqualsNumber() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", 12.34));
        return query;
    }

    protected Query buildQueryFilterEqualsString() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "=",
            "testFilterValue"));
        return query;
    }

    protected Query buildQueryFilterEqualsZero() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", 0));
        return query;
    }

    protected Query buildQueryFilterNotEqualsBoolean() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromBoolean(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=", true));
        return query;
    }

    protected Query buildQueryFilterNotEqualsDate() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDate(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z")));
        return query;
    }

    protected Query buildQueryFilterNotEqualsEmpty() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=", ""));
        return query;
    }

    protected Query buildQueryFilterNotEqualsFalse() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromBoolean(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=", false));
        return query;
    }

    protected Query buildQueryFilterNotEqualsNull() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromNull(new FieldClause("testDatabase", "testTable", "testFilterField"), "!="));
        return query;
    }

    protected Query buildQueryFilterNotEqualsNumber() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=", 12.34));
        return query;
    }

    protected Query buildQueryFilterNotEqualsString() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=",
            "testFilterValue"));
        return query;
    }

    protected Query buildQueryFilterNotEqualsZero() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=", 0));
        return query;
    }

    protected Query buildQueryFilterGreaterThan() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), ">", 12.34));
        return query;
    }

    protected Query buildQueryFilterGreaterThanOrEqualTo() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), ">=", 12.34));
        return query;
    }

    protected Query buildQueryFilterLessThan() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), "<", 12.34));
        return query;
    }

    protected Query buildQueryFilterLessThanOrEqualTo() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), "<=", 12.34));
        return query;
    }

    protected Query buildQueryFilterContains() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "contains",
            "testFilterValue"));
        return query;
    }

    protected Query buildQueryFilterNotContains() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "not contains",
            "testFilterValue"));
        return query;
    }

    protected Query buildQueryFilterAnd() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField1"), "=", "testFilterValue1"),
            SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField2"), "=", "testFilterValue2")
        )));
        return query;
    }

    protected Query buildQueryFilterOr() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(new OrWhereClause(Arrays.asList(
            SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField1"), "=", "testFilterValue1"),
            SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField2"), "=", "testFilterValue2")
        )));
        return query;
    }

    protected Query buildQueryAggregateAvg() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "avg")));
        return query;
    }

    protected Query buildQueryAggregateCountAll() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByTotalCountClause("testAggLabel")));
        return query;
    }

    protected Query buildQueryAggregateCountField() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "count")));
        return query;
    }

    protected Query buildQueryAggregateMax() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "max")));
        return query;
    }

    protected Query buildQueryAggregateMin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "min")));
        return query;
    }

    protected Query buildQueryAggregateSum() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "sum")));
        return query;
    }

    protected Query buildQueryMultipleAggregate() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField1"), "testAggLabel1", "avg"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField2"), "testAggLabel2", "sum")
        ));
        return query;
    }

    protected Query buildQueryGroupByField() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        return query;
    }

    protected Query buildQueryGroupByFieldSparql() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "o"))));
        return query;
    }

    protected Query buildQueryGroupByDateSecond() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "second")));
        return query;
    }

    protected Query buildQueryGroupByDateMinute() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "minute")));
        return query;
    }

    protected Query buildQueryGroupByDateHour() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "hour")));
        return query;
    }

    protected Query buildQueryGroupByDateDay() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "dayOfMonth")));
        return query;
    }

    protected Query buildQueryGroupByDateMonth() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "month")));
        return query;
    }

    protected Query buildQueryGroupByDateYear() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "year")));
        return query;
    }

    protected Query buildQueryMultipleGroupByField() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField1")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField2")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField3"))
        ));
        return query;
    }

    protected Query buildQueryMultipleGroupByDate() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField1"), "testGroupLabel1", "minute"),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField2"), "testGroupLabel2", "hour"),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField3"), "testGroupLabel3", "dayOfMonth"),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField4"), "testGroupLabel4", "month"),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField5"), "testGroupLabel5", "year")
        ));
        return query;
    }

    protected Query buildQueryMultipleGroupByDateAndField() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField1")),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField2"), "testGroupLabel2", "year"),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField3")),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField4"), "testGroupLabel4", "month")
        ));
        return query;
    }

    protected Query buildQueryAggregateAndGroup() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "sum")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        return query;
    }

    protected Query buildQueryAggregateCountGroup() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByGroupCountClause("testGroupLabel", "testAggLabel")));
        query.setGroupByClauses(Arrays.asList(new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "year")));
        return query;
    }

    protected Query buildQueryMultipleAggregateAndGroup() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField1"), "testAggLabel1", "avg"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField2"), "testAggLabel2", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField1")),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField2"), "testGroupLabel2", "year"),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField3")),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField4"), "testGroupLabel4", "month")
        ));
        return query;
    }

    protected Query buildQueryAggregateAndGroupByDate() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "sum")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField1"), "testGroupLabel1", "year"),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField2"), "testGroupLabel2", "month")
        ));
        return query;
    }

    protected Query buildQuerySortAscending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testSortField"), Order.ASCENDING)));
        return query;
    }

    protected Query buildQuerySortAscendingSparql() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "o"), Order.ASCENDING)));
        return query;
    }

    protected Query buildQuerySortDescending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testSortField"), Order.DESCENDING)));
        return query;
    }

    protected Query buildQuerySortDescendingSparql() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "o"), Order.DESCENDING)));
        return query;
    }

    protected Query buildQuerySortOnAggregationAscending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testField"), "testAggLabel", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testField"))
        ));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testAggLabel", Order.ASCENDING)));
        return query;
    }

    protected Query buildQuerySortOnAggregationDescending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testField"), "testAggLabel", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testField"))
        ));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testAggLabel", Order.DESCENDING)));
        return query;
    }

    protected Query buildQuerySortOnGroupOperationAscending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "year")
        ));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testGroupLabel", Order.ASCENDING)));
        return query;
    }

    protected Query buildQuerySortOnGroupOperationDescending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "year")
        ));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testGroupLabel", Order.DESCENDING)));
        return query;
    }

    protected Query buildQueryMultipleSort() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setOrderByClauses(Arrays.asList(
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testSortField1"), Order.ASCENDING),
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testSortField2"), Order.DESCENDING)
        ));
        return query;
    }

    protected Query buildQueryAggregateAndGroupAndSort() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testField1"), "testAggLabel1", "avg"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testField2"), "testAggLabel2", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testField1")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testField2"))
        ));
        query.setOrderByClauses(Arrays.asList(
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testField1"), Order.ASCENDING),
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testField2"), Order.DESCENDING)
        ));
        return query;
    }

    protected Query buildQueryAggregateAndSort() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testField1"), "testAggLabel1", "avg"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testField2"), "testAggLabel2", "sum")
        ));
        query.setOrderByClauses(Arrays.asList(
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testField1"), Order.ASCENDING),
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testField3"), Order.DESCENDING)
        ));
        return query;
    }

    protected Query buildQueryGroupByFieldAndSort() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testField1")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testField2"))
        ));
        query.setOrderByClauses(Arrays.asList(
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testField1"), Order.ASCENDING),
            new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testField3"), Order.DESCENDING)
        ));
        return query;
    }

    protected Query buildQueryGroupByFunctionAndSort() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField1"), "testGroupLabel1", "year"),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField2"), "testGroupLabel2", "month")
        ));
        query.setOrderByClauses(Arrays.asList(
            new OrderByOperationClause("testGroupLabel1", Order.ASCENDING),
            new OrderByOperationClause("testGroupLabel2", Order.DESCENDING)
        ));
        return query;
    }

    protected Query buildQueryLimit() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(12));
        return query;
    }

    protected Query buildQueryOffset() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setOffsetClause(new OffsetClause(34));
        return query;
    }

    protected Query buildQueryLimitAndOffset() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(12));
        query.setOffsetClause(new OffsetClause(34));
        return query;
    }

    protected Query buildQueryAdvanced() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testField1"),
            new FieldClause("testDatabase", "testTable", "testField2")
        )));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "sum")));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", "testFilterValue"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(12));
        query.setOffsetClause(new OffsetClause(34));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testSortField"), Order.ASCENDING)));
        return query;
    }

    protected Query buildQueryDuplicateFields() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testField1"),
            new FieldClause("testDatabase", "testTable", "testField2"),
            new FieldClause("testDatabase", "testTable", "testField3"),
            new FieldClause("testDatabase", "testTable", "testField3")
        )));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testField1"), "testAggLabel", "sum")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testField2"))));
        return query;
    }

    protected Query buildQueryFilterDateRange() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDate(new FieldClause("testDatabase", "testTable", "testFilterField"), ">=", DateUtil.transformStringToDate("2018-01-01T00:00Z")),
            SingularWhereClause.fromDate(new FieldClause("testDatabase", "testTable", "testFilterField"), "<=", DateUtil.transformStringToDate("2019-01-01T00:00Z"))
        )));
        return query;
    }

    protected Query buildQueryFilterNumberRange() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), ">=", 12.34),
            SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTable", "testFilterField"), "<=", 56.78)
        )));
        return query;
    }

    protected Query buildQueryFilterNestedAnd() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(new AndWhereClause(Arrays.asList(
            new OrWhereClause(Arrays.asList(
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField1"), "=", "testFilterValue1"),
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField2"), "=", "testFilterValue2")
            )),
            new OrWhereClause(Arrays.asList(
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField3"), "=", "testFilterValue3"),
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField4"), "=", "testFilterValue4")
            ))
        )));
        return query;
    }

    protected Query buildQueryFilterNestedOr() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(new OrWhereClause(Arrays.asList(
            new AndWhereClause(Arrays.asList(
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField1"), "=", "testFilterValue1"),
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField2"), "=", "testFilterValue2")
            )),
            new AndWhereClause(Arrays.asList(
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField3"), "=", "testFilterValue3"),
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField4"), "=", "testFilterValue4")
            ))
        )));
        return query;
    }

    protected Query buildQueryAggregateCountFieldAndFilter() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "=",
            "testFilterValue"));
        query.setAggregateClauses(Arrays.asList(new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel", "count")));
        return query;
    }

    protected Query buildQueryAggregateCountFieldAndMetrics() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel1", "count"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel2", "sum")
        ));
        return query;
    }

    protected Query buildQueryJoin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(new JoinClause("", "testDatabase", "testTableB", new FieldsWhereClause(
            new FieldClause("testDatabase", "testTableA", "testField1"),
            "=",
            new FieldClause("testDatabase", "testTableB", "testField2")
        ))));
        return query;
    }

    protected Query buildQueryCrossJoin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(new JoinClause("cross", "testDatabase", "testTableB", new FieldsWhereClause(
            new FieldClause("testDatabase", "testTableA", "testField1"),
            "=",
            new FieldClause("testDatabase", "testTableB", "testField2")
        ))));
        return query;
    }

    protected Query buildQueryFullJoin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(new JoinClause("full", "testDatabase", "testTableB", new FieldsWhereClause(
            new FieldClause("testDatabase", "testTableA", "testField1"),
            "=",
            new FieldClause("testDatabase", "testTableB", "testField2")
        ))));
        return query;
    }

    protected Query buildQueryInnerJoin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(new JoinClause("inner", "testDatabase", "testTableB", new FieldsWhereClause(
            new FieldClause("testDatabase", "testTableA", "testField1"),
            "=",
            new FieldClause("testDatabase", "testTableB", "testField2")
        ))));
        return query;
    }

    protected Query buildQueryLeftJoin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(new JoinClause("left", "testDatabase", "testTableB", new FieldsWhereClause(
            new FieldClause("testDatabase", "testTableA", "testField1"),
            "=",
            new FieldClause("testDatabase", "testTableB", "testField2")
        ))));
        return query;
    }

    protected Query buildQueryRightJoin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(new JoinClause("right", "testDatabase", "testTableB", new FieldsWhereClause(
            new FieldClause("testDatabase", "testTableA", "testField1"),
            "=",
            new FieldClause("testDatabase", "testTableB", "testField2")
        ))));
        return query;
    }

    protected Query buildQueryJoinWithCompoundOn() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(new JoinClause("", "testDatabase", "testTableB",
            new AndWhereClause(Arrays.asList(
                new FieldsWhereClause(new FieldClause("testDatabase", "testTableA", "testField1"), "=",
                    new FieldClause("testDatabase", "testTableB", "testField2")),
                SingularWhereClause.fromDouble(new FieldClause("testDatabase", "testTableA", "testField3"), "!=", 0),
                SingularWhereClause.fromString(new FieldClause("testDatabase", "testTableB", "testField4"), "=", "a"),
                new OrWhereClause(Arrays.asList(
                    SingularWhereClause.fromDate(new FieldClause("testDatabase", "testTableA", "testField5"), ">",
                        DateUtil.transformStringToDate("2019-01-01T00:00Z")),
                    SingularWhereClause.fromNull(new FieldClause("testDatabase", "testTableB", "testField6"), "!=")
                ))
            ))
        )));
        return query;
    }

    protected Query buildQueryMultipleJoin() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTableA"));
        query.setJoinClauses(Arrays.asList(
            new JoinClause("", "testDatabase", "testTableB", new FieldsWhereClause(
                new FieldClause("testDatabase", "testTableA", "testField1"),
                "=",
                new FieldClause("testDatabase", "testTableB", "testField2")
            )),
            new JoinClause("", "testDatabase", "testTableC", new FieldsWhereClause(
                new FieldClause("testDatabase", "testTableA", "testField1"),
                "=",
                new FieldClause("testDatabase", "testTableC", "testField3")
            ))
        ));
        return query;
    }

    protected MutateQuery buildMutationByIdQuery() {
        return new MutateQuery("testHost", "testType", "testDatabase", "testTable", "testIdField", "testId",
            new LinkedHashMap<String, Object>(){{
                put("testString", "a");
                put("testZero", 0);
                put("testInteger", 1);
                put("testDecimal", 0.5);
                put("testNegativeInteger", -1);
                put("testNegativeDecimal", -0.5);
                put("testTrue", true);
                put("testFalse", false);
            }});
    }

    protected MutateQuery buildArrayAndObjectMutationByIdQuery() {
        return new MutateQuery("testHost", "testType", "testDatabase", "testTable", "testIdField", "testId",
            new LinkedHashMap<String, Object>(){{
                put("testEmptyArray", new ArrayList<Object>());
                put("testEmptyObject", new LinkedHashMap<String, Object>());
                put("testArray", new ArrayList<Object>() {{
                    add("b");
                    add(2);
                    add(true);
                    add(new LinkedHashMap<String, Object>() {{
                        put("testArrayObjectString", "c");
                        put("testArrayObjectInteger", 3);
                    }});
                }});
                put("testObject", new LinkedHashMap<String, Object>() {{
                    put("testObjectString", "d");
                    put("testObjectInteger", 4);
                    put("testObjectBoolean", true);
                    put("testObjectArray", new ArrayList<Object>() {{
                        add("e");
                        add(5);
                    }});
                }});
            }});
    }
}
