package com.ncc.neon.adapters;

import java.util.Arrays;

import com.ncc.neon.models.queries.AggregateByFieldClause;
import com.ncc.neon.models.queries.AggregateByGroupCountClause;
import com.ncc.neon.models.queries.AggregateByTotalCountClause;
import com.ncc.neon.models.queries.AndWhereClause;
import com.ncc.neon.models.queries.FieldClause;
import com.ncc.neon.models.queries.GroupByFieldClause;
import com.ncc.neon.models.queries.GroupByOperationClause;
import com.ncc.neon.models.queries.LimitClause;
import com.ncc.neon.models.queries.OffsetClause;
import com.ncc.neon.models.queries.OrWhereClause;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.SelectClause;
import com.ncc.neon.models.queries.SingularWhereClause;
import com.ncc.neon.models.queries.OrderByFieldClause;
import com.ncc.neon.models.queries.OrderByGroupClause;
import com.ncc.neon.models.queries.Order;
import com.ncc.neon.util.DateUtil;

/**
 * Builds queries for the query adapters' unit tests.
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

    protected Query buildQuerySortDescending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testSortField"), Order.DESCENDING)));
        return query;
    }

    protected Query buildQuerySortOnGroupAscending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "year")
        ));
        query.setOrderByClauses(Arrays.asList(new OrderByGroupClause("testGroupLabel", Order.ASCENDING)));
        return query;
    }

    protected Query buildQuerySortOnGroupDescending() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testGroupLabel", "year")
        ));
        query.setOrderByClauses(Arrays.asList(new OrderByGroupClause("testGroupLabel", Order.DESCENDING)));
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
            new OrderByGroupClause("testGroupLabel1", Order.ASCENDING),
            new OrderByGroupClause("testGroupLabel2", Order.DESCENDING)
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
}
