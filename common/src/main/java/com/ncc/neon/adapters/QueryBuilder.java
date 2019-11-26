package com.ncc.neon.adapters;

import java.util.Arrays;

import com.ncc.neon.models.queries.AggregateClause;
import com.ncc.neon.models.queries.AndWhereClause;
import com.ncc.neon.models.queries.Filter;
import com.ncc.neon.models.queries.GroupByFieldClause;
import com.ncc.neon.models.queries.GroupByFunctionClause;
import com.ncc.neon.models.queries.LimitClause;
import com.ncc.neon.models.queries.OffsetClause;
import com.ncc.neon.models.queries.OrWhereClause;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.SingularWhereClause;
import com.ncc.neon.models.queries.SortClause;
import com.ncc.neon.models.queries.SortClauseOrder;
import com.ncc.neon.util.DateUtil;

/**
 * Builds queries for the query adapters' unit tests.
 * The tests can't seem to find this file unless it's on the "src/main/java" path.
 */
public class QueryBuilder {

    protected Query buildQueryBase() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        return query;
    }

    protected Query buildQueryFields() {
        Query query = new Query();
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        return query;
    }

    protected Query buildQueryDistinct() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testField1"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        return query;
    }

    protected Query buildQueryFilterEqualsBoolean() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", true)));
        return query;
    }

    protected Query buildQueryFilterEqualsDate() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDate("testFilterField", "=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z"))));
        return query;
    }

    protected Query buildQueryFilterEqualsEmpty() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "")));
        return query;
    }

    protected Query buildQueryFilterEqualsFalse() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", false)));
        return query;
    }

    protected Query buildQueryFilterEqualsNull() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromNull("testFilterField", "=")));
        return query;
    }

    protected Query buildQueryFilterEqualsNumber() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 12.34)));
        return query;
    }

    protected Query buildQueryFilterEqualsString() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=",
            "testFilterValue")));
        return query;
    }

    protected Query buildQueryFilterEqualsZero() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 0)));
        return query;
    }

    protected Query buildQueryFilterNotEqualsBoolean() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "!=", true)));
        return query;
    }

    protected Query buildQueryFilterNotEqualsDate() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDate("testFilterField", "!=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z"))));
        return query;
    }

    protected Query buildQueryFilterNotEqualsEmpty() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "!=", "")));
        return query;
    }

    protected Query buildQueryFilterNotEqualsFalse() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "!=", false)));
        return query;
    }

    protected Query buildQueryFilterNotEqualsNull() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromNull("testFilterField", "!=")));
        return query;
    }

    protected Query buildQueryFilterNotEqualsNumber() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "!=", 12.34)));
        return query;
    }

    protected Query buildQueryFilterNotEqualsString() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "!=",
            "testFilterValue")));
        return query;
    }

    protected Query buildQueryFilterNotEqualsZero() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "!=", 0)));
        return query;
    }

    protected Query buildQueryFilterGreaterThan() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", ">", 12.34)));
        return query;
    }

    protected Query buildQueryFilterGreaterThanOrEqualTo() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", ">=", 12.34)));
        return query;
    }

    protected Query buildQueryFilterLessThan() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "<", 12.34)));
        return query;
    }

    protected Query buildQueryFilterLessThanOrEqualTo() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "<=", 12.34)));
        return query;
    }

    protected Query buildQueryFilterContains() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "contains",
            "testFilterValue")));
        return query;
    }

    protected Query buildQueryFilterNotContains() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "not contains",
            "testFilterValue")));
        return query;
    }

    protected Query buildQueryFilterAnd() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));
        return query;
    }

    protected Query buildQueryFilterOr() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new OrWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));
        return query;
    }

    protected Query buildQueryAggregateAvg() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "avg", "testAggField")));
        return query;
    }

    protected Query buildQueryAggregateCountAll() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "*")));
        return query;
    }

    protected Query buildQueryAggregateCountField() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "testAggField")));
        return query;
    }

    protected Query buildQueryAggregateMax() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "max", "testAggField")));
        return query;
    }

    protected Query buildQueryAggregateMin() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "min", "testAggField")));
        return query;
    }

    protected Query buildQueryAggregateSum() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));
        return query;
    }

    protected Query buildQueryMultipleAggregate() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "avg", "testAggField1"),
            new AggregateClause("testAggName2", "sum", "testAggField2")
        ));
        return query;
    }

    protected Query buildQueryGroupByField() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        return query;
    }

    protected Query buildQueryGroupByDateSecond() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "second", "testGroupField")));
        return query;
    }

    protected Query buildQueryGroupByDateMinute() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "minute", "testGroupField")));
        return query;
    }

    protected Query buildQueryGroupByDateHour() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "hour", "testGroupField")));
        return query;
    }

    protected Query buildQueryGroupByDateDay() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "dayOfMonth", "testGroupField")));
        return query;
    }

    protected Query buildQueryGroupByDateMonth() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "month", "testGroupField")));
        return query;
    }

    protected Query buildQueryGroupByDateYear() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "year", "testGroupField")));
        return query;
    }

    protected Query buildQueryMultipleGroupByField() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFieldClause("testGroupField2", "Test Group Field 2"),
            new GroupByFieldClause("testGroupField3", "Test Group Field 3")
        ));
        return query;
    }

    protected Query buildQueryMultipleGroupByDate() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFunctionClause("testGroupName1", "minute", "testGroupField1"),
            new GroupByFunctionClause("testGroupName2", "hour", "testGroupField2"),
            new GroupByFunctionClause("testGroupName3", "dayOfMonth", "testGroupField3"),
            new GroupByFunctionClause("testGroupName4", "month", "testGroupField4"),
            new GroupByFunctionClause("testGroupName5", "year", "testGroupField5")
        ));
        return query;
    }

    protected Query buildQueryMultipleGroupByDateAndField() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFunctionClause("testGroupName2", "year", "testGroupField2"),
            new GroupByFieldClause("testGroupField3", "Test Group Field 3"),
            new GroupByFunctionClause("testGroupName4", "month", "testGroupField4")
        ));
        return query;
    }

    protected Query buildQueryAggregateAndGroup() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        return query;
    }

    protected Query buildQueryMultipleAggregateAndGroup() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "avg", "testAggField1"),
            new AggregateClause("testAggName2", "sum", "testAggField2")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFieldClause("testGroupField2", "Test Group Field 2")
        ));
        return query;
    }

    protected Query buildQueryAggregateAndGroupByDate() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFunctionClause("testGroupName1", "year", "testGroupField1"),
            new GroupByFunctionClause("testGroupName2", "month", "testGroupField2")
        ));
        return query;
    }

    protected Query buildQuerySortAscending() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortClauseOrder.ASCENDING)));
        return query;
    }

    protected Query buildQuerySortDescending() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortClauseOrder.DESCENDING)));
        return query;
    }

    protected Query buildQueryMultipleSort() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(
            new SortClause("testSortField1", SortClauseOrder.ASCENDING),
            new SortClause("testSortField2", SortClauseOrder.DESCENDING)
        ));
        return query;
    }

    protected Query buildQueryAggregateAndGroupAndSort() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "avg", "testField1"),
            new AggregateClause("testAggName2", "sum", "testField2")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testField1", "Test Field 1"),
            new GroupByFieldClause("testField2", "Test Field 2")
        ));
        query.setSortClauses(Arrays.asList(
            new SortClause("testField1", SortClauseOrder.ASCENDING),
            new SortClause("testField2", SortClauseOrder.DESCENDING),
            new SortClause("testField4", SortClauseOrder.ASCENDING)
        ));
        return query;
    }

    protected Query buildQueryAggregateAndSort() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "avg", "testField1"),
            new AggregateClause("testAggName2", "sum", "testField2")
        ));
        query.setSortClauses(Arrays.asList(
            new SortClause("testField1", SortClauseOrder.ASCENDING),
            new SortClause("testField2", SortClauseOrder.DESCENDING),
            new SortClause("testField4", SortClauseOrder.ASCENDING)
        ));
        return query;
    }

    protected Query buildQueryGroupAndSort() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testField1", "Test Field 1"),
            new GroupByFieldClause("testField2", "Test Field 2"),
            new GroupByFieldClause("testField3", "Test Field 3")
        ));
        query.setSortClauses(Arrays.asList(
            new SortClause("testField1", SortClauseOrder.ASCENDING),
            new SortClause("testField2", SortClauseOrder.DESCENDING),
            new SortClause("testField4", SortClauseOrder.ASCENDING)
        ));
        return query;
    }

    protected Query buildQueryLimit() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(12));
        return query;
    }

    protected Query buildQueryOffset() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setOffsetClause(new OffsetClause(34));
        return query;
    }

    protected Query buildQueryLimitAndOffset() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(12));
        query.setOffsetClause(new OffsetClause(34));
        return query;
    }

    protected Query buildQueryAdvanced() {
        Query query = new Query();
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "testFilterValue")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field 1")));
        query.setLimitClause(new LimitClause(12));
        query.setOffsetClause(new OffsetClause(34));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortClauseOrder.ASCENDING)));
        return query;
    }

    protected Query buildQueryDuplicateFields() {
        Query query = new Query();
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testField1")));
        query.setFields(Arrays.asList("testField1", "testField2", "testField3", "testField3"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testField2", "Test Field 2")));
        return query;
    }

    protected Query buildQueryFilterDateRange() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDate("testFilterField", ">=", DateUtil.transformStringToDate("2018-01-01T00:00Z")),
            SingularWhereClause.fromDate("testFilterField", "<=", DateUtil.transformStringToDate("2019-01-01T00:00Z"))
        ))));
        return query;
    }

    protected Query buildQueryFilterNumberRange() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDouble("testFilterField", ">=", 12.34),
            SingularWhereClause.fromDouble("testFilterField", "<=", 56.78)
        ))));
        return query;
    }

    protected Query buildQueryFilterNestedAnd() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            new OrWhereClause(Arrays.asList(
                SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
                SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
            )),
            new OrWhereClause(Arrays.asList(
                SingularWhereClause.fromString("testFilterField3", "=", "testFilterValue3"),
                SingularWhereClause.fromString("testFilterField4", "=", "testFilterValue4")
            ))
        ))));
        return query;
    }

    protected Query buildQueryFilterNestedOr() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new OrWhereClause(Arrays.asList(
            new AndWhereClause(Arrays.asList(
                SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
                SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
            )),
            new AndWhereClause(Arrays.asList(
                SingularWhereClause.fromString("testFilterField3", "=", "testFilterValue3"),
                SingularWhereClause.fromString("testFilterField4", "=", "testFilterValue4")
            ))
        ))));
        return query;
    }

    protected Query buildQueryAggregateCountFieldAndFilter() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=",
            "testFilterValue")));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "testAggField")));
        return query;
    }

    protected Query buildQueryAggregateCountFieldAndMetrics() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "count", "testAggField"),
            new AggregateClause("testAggName2", "sum", "testAggField")
        ));
        return query;
    }
}
