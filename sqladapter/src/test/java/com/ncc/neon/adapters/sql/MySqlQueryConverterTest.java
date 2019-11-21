package com.ncc.neon.adapters.sql;

import static org.assertj.core.api.Assertions.assertThat;

import com.ncc.neon.adapters.QueryBuilder;
import com.ncc.neon.models.queries.Filter;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.SingularWhereClause;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=SqlQueryConverter.class)
public class MySqlQueryConverterTest extends QueryBuilder {

    @Test
    public void convertQueryBaseTest() {
        Query query = buildQueryBase();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFieldsTest() {
        Query query = buildQueryFields();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testField1, testField2 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryDistinctTest() {
        Query query = buildQueryDistinct();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT DISTINCT testField1 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsBooleanTest() {
        Query query = buildQueryFilterEqualsBoolean();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsDateTest() {
        Query query = buildQueryFilterEqualsDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField = " +
            "STR_TO_DATE('2019-01-01T00:00:00Z','%Y-%m-%dT%TZ')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsEmptyTest() {
        Query query = buildQueryFilterEqualsEmpty();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField = ''";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsFalseTest() {
        Query query = buildQueryFilterEqualsFalse();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE NOT testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNullTest() {
        Query query = buildQueryFilterEqualsNull();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField IS NULL";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNumberTest() {
        Query query = buildQueryFilterEqualsNumber();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField = 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsStringTest() {
        Query query = buildQueryFilterEqualsString();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField = 'testFilterValue'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsZeroTest() {
        Query query = buildQueryFilterEqualsZero();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField = 0.0";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsBooleanTest() {
        Query query = buildQueryFilterNotEqualsBoolean();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE NOT testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsDateTest() {
        Query query = buildQueryFilterNotEqualsDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField != " +
            "STR_TO_DATE('2019-01-01T00:00:00Z','%Y-%m-%dT%TZ')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsEmptyTest() {
        Query query = buildQueryFilterNotEqualsEmpty();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField != ''";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsFalseTest() {
        Query query = buildQueryFilterNotEqualsFalse();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNullTest() {
        Query query = buildQueryFilterNotEqualsNull();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField IS NOT NULL";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNumberTest() {
        Query query = buildQueryFilterNotEqualsNumber();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField != 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsStringTest() {
        Query query = buildQueryFilterNotEqualsString();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField != 'testFilterValue'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsZeroTest() {
        Query query = buildQueryFilterNotEqualsZero();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField != 0.0";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanTest() {
        Query query = buildQueryFilterGreaterThan();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField > 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanOrEqualToTest() {
        Query query = buildQueryFilterGreaterThanOrEqualTo();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField >= 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanTest() {
        Query query = buildQueryFilterLessThan();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField < 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanOrEqualToTest() {
        Query query = buildQueryFilterLessThanOrEqualTo();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField <= 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterContainsTest() {
        Query query = buildQueryFilterContains();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField REGEXP '.*testFilterValue.*'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotContainsTest() {
        Query query = buildQueryFilterNotContains();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField NOT REGEXP '.*testFilterValue.*'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterAndTest() {
        Query query = buildQueryFilterAnd();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testFilterField1 = 'testFilterValue1' AND " +
            "testFilterField2 = 'testFilterValue2')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterOrTest() {
        Query query = buildQueryFilterOr();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testFilterField1 = 'testFilterValue1' OR " +
            "testFilterField2 = 'testFilterValue2')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAvgTest() {
        Query query = buildQueryAggregateAvg();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testAggField) AS testAggName FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountAllTest() {
        Query query = buildQueryAggregateCountAll();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(*) AS testAggName FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldTest() {
        Query query = buildQueryAggregateCountField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(testAggField) AS testAggName FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMaxTest() {
        Query query = buildQueryAggregateMax();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MAX(testAggField) AS testAggName FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMinTest() {
        Query query = buildQueryAggregateMin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MIN(testAggField) AS testAggName FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateSumTest() {
        Query query = buildQueryAggregateSum();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testAggField) AS testAggName FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateTest() {
        Query query = buildQueryMultipleAggregate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testAggField1) AS testAggName1, SUM(testAggField2) AS testAggName2 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFieldTest() {
        Query query = buildQueryGroupByField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testGroupField FROM testDatabase.testTable GROUP BY testGroupField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateSecondTest() {
        Query query = buildQueryGroupByDateSecond();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SECOND(testGroupField) AS testGroupName FROM testDatabase.testTable GROUP BY testGroupName";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMinuteTest() {
        Query query = buildQueryGroupByDateMinute();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MINUTE(testGroupField) AS testGroupName FROM testDatabase.testTable GROUP BY testGroupName";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateHourTest() {
        Query query = buildQueryGroupByDateHour();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT HOUR(testGroupField) AS testGroupName FROM testDatabase.testTable GROUP BY testGroupName";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateDayTest() {
        Query query = buildQueryGroupByDateDay();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT DAYOFMONTH(testGroupField) AS testGroupName FROM testDatabase.testTable GROUP BY testGroupName";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMonthTest() {
        Query query = buildQueryGroupByDateMonth();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MONTH(testGroupField) AS testGroupName FROM testDatabase.testTable GROUP BY testGroupName";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateYearTest() {
        Query query = buildQueryGroupByDateYear();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT YEAR(testGroupField) AS testGroupName FROM testDatabase.testTable GROUP BY testGroupName";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByFieldTest() {
        Query query = buildQueryMultipleGroupByField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testGroupField1, testGroupField2, testGroupField3 FROM testDatabase.testTable " +
            "GROUP BY testGroupField1, testGroupField2, testGroupField3";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByDateTest() {
        Query query = buildQueryMultipleGroupByDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MINUTE(testGroupField1) AS testGroupName1, HOUR(testGroupField2) AS testGroupName2, " +
            "DAYOFMONTH(testGroupField3) AS testGroupName3, MONTH(testGroupField4) AS testGroupName4, " +
            "YEAR(testGroupField5) AS testGroupName5 FROM testDatabase.testTable GROUP BY testGroupName1, " +
            "testGroupName2, testGroupName3, testGroupName4, testGroupName5";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByDateAndFieldTest() {
        Query query = buildQueryMultipleGroupByDateAndField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testGroupField1, YEAR(testGroupField2) AS testGroupName2, testGroupField3, " +
            "MONTH(testGroupField4) AS testGroupName4 FROM testDatabase.testTable GROUP BY testGroupField1, " +
            "testGroupName2, testGroupField3, testGroupName4";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupTest() {
        Query query = buildQueryAggregateAndGroup();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testAggField) AS testAggName, testGroupField FROM testDatabase.testTable GROUP BY testGroupField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateAndGroupTest() {
        Query query = buildQueryMultipleAggregateAndGroup();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testAggField1) AS testAggName1, SUM(testAggField2) AS testAggName2, " +
            "testGroupField1, testGroupField2 FROM testDatabase.testTable GROUP BY testGroupField1, testGroupField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupByDateTest() {
        Query query = buildQueryAggregateAndGroupByDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testAggField) AS testAggName, YEAR(testGroupField1) AS testGroupName1, " +
            "MONTH(testGroupField2) AS testGroupName2 FROM testDatabase.testTable GROUP BY testGroupName1, " +
            "testGroupName2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortAscendingTest() {
        Query query = buildQuerySortAscending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable ORDER BY testSortField ASC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortDescendingTest() {
        Query query = buildQuerySortDescending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable ORDER BY testSortField DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleSortTest() {
        Query query = buildQueryMultipleSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable ORDER BY testSortField1 ASC, testSortField2 DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupAndSortTest() {
        Query query = buildQueryAggregateAndGroupAndSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testField1) AS testAggName1, SUM(testField2) AS testAggName2, testField1, " +
            "testField2 FROM testDatabase.testTable GROUP BY testField1, testField2 ORDER BY testField1 ASC, " +
            "testField2 DESC, testField4 ASC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndSortTest() {
        Query query = buildQueryAggregateAndSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testField1) AS testAggName1, SUM(testField2) AS testAggName2 FROM " +
            "testDatabase.testTable ORDER BY testField1 ASC, testField2 DESC, testField4 ASC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupAndSortTest() {
        Query query = buildQueryGroupAndSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testField1, testField2, testField3 FROM testDatabase.testTable GROUP BY " +
            "testField1, testField2, testField3 ORDER BY testField1 ASC, testField2 DESC, testField4 ASC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitTest() {
        Query query = buildQueryLimit();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable LIMIT 12";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryOffsetTest() {
        Query query = buildQueryOffset();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = null;
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitAndOffsetTest() {
        Query query = buildQueryLimitAndOffset();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable LIMIT 12 OFFSET 34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAdvancedTest() {
        Query query = buildQueryAdvanced();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testField1, testField2, SUM(testAggField) AS testAggName, testGroupField FROM " +
            "testDatabase.testTable WHERE testFilterField = 'testFilterValue' GROUP BY testGroupField ORDER BY " +
            "testSortField ASC LIMIT 12 OFFSET 34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryDuplicateFieldsTest() {
        Query query = buildQueryDuplicateFields();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.POSTGRESQL);
        String expected = "SELECT testField1, testField2, testField3, SUM(testField1) AS testAggName FROM " +
            "testDatabase.testTable GROUP BY testField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterDateRangeTest() {
        Query query = buildQueryFilterDateRange();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testFilterField >= " +
            "STR_TO_DATE('2018-01-01T00:00:00Z','%Y-%m-%dT%TZ') AND testFilterField <= " +
            "STR_TO_DATE('2019-01-01T00:00:00Z','%Y-%m-%dT%TZ'))";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNumberRangeTest() {
        Query query = buildQueryFilterNumberRange();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testFilterField >= 12.34 AND testFilterField <= 56.78)";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNestedAndTest() {
        Query query = buildQueryFilterNestedAnd();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE ((testFilterField1 = 'testFilterValue1' OR " +
            "testFilterField2 = 'testFilterValue2') AND (testFilterField3 = 'testFilterValue3' OR " +
            "testFilterField4 = 'testFilterValue4'))";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNestedOrTest() {
        Query query = buildQueryFilterNestedOr();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE ((testFilterField1 = 'testFilterValue1' AND " +
            "testFilterField2 = 'testFilterValue2') OR (testFilterField3 = 'testFilterValue3' AND " +
            "testFilterField4 = 'testFilterValue4'))";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldAndFilterTest() {
        Query query = buildQueryAggregateCountFieldAndFilter();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(testAggField) AS testAggName FROM testDatabase.testTable WHERE testFilterField = 'testFilterValue'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldAndMetricsTest() {
        Query query = buildQueryAggregateCountFieldAndMetrics();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(testAggField) AS testAggName1, SUM(testAggField) AS testAggName2 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterContainsQuotesTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "contains",
            "'; SQL INJECTION; '")));
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField REGEXP '.*\\'; SQL INJECTION; \\'.*'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsQuotesTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=",
            "'; SQL INJECTION; '")));
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testFilterField = '\\'; SQL INJECTION; \\''";
        assertThat(actual).isEqualTo(expected);
    }
}
