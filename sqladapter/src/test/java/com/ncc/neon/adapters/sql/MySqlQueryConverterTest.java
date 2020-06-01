package com.ncc.neon.adapters.sql;

import com.ncc.neon.adapters.QueryBuilder;
import com.ncc.neon.models.queries.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

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
        String expected = "SELECT testDatabase.testTable.testField1, testDatabase.testTable.testField2 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryDistinctTest() {
        Query query = buildQueryDistinct();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT DISTINCT testDatabase.testTable.testField1 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsBooleanTest() {
        Query query = buildQueryFilterEqualsBoolean();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsDateTest() {
        Query query = buildQueryFilterEqualsDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField = " +
            "STR_TO_DATE('2019-01-01T00:00:00Z','%Y-%m-%dT%TZ')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsEmptyTest() {
        Query query = buildQueryFilterEqualsEmpty();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField = ''";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsFalseTest() {
        Query query = buildQueryFilterEqualsFalse();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE NOT testDatabase.testTable.testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNullTest() {
        Query query = buildQueryFilterEqualsNull();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField IS NULL";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNumberTest() {
        Query query = buildQueryFilterEqualsNumber();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField = 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsStringTest() {
        Query query = buildQueryFilterEqualsString();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField = 'testFilterValue'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsZeroTest() {
        Query query = buildQueryFilterEqualsZero();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField = 0.0";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsBooleanTest() {
        Query query = buildQueryFilterNotEqualsBoolean();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE NOT testDatabase.testTable.testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsDateTest() {
        Query query = buildQueryFilterNotEqualsDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField != " +
            "STR_TO_DATE('2019-01-01T00:00:00Z','%Y-%m-%dT%TZ')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsEmptyTest() {
        Query query = buildQueryFilterNotEqualsEmpty();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField != ''";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsFalseTest() {
        Query query = buildQueryFilterNotEqualsFalse();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNullTest() {
        Query query = buildQueryFilterNotEqualsNull();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField IS NOT NULL";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNumberTest() {
        Query query = buildQueryFilterNotEqualsNumber();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField != 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsStringTest() {
        Query query = buildQueryFilterNotEqualsString();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField != 'testFilterValue'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsZeroTest() {
        Query query = buildQueryFilterNotEqualsZero();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField != 0.0";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanTest() {
        Query query = buildQueryFilterGreaterThan();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField > 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanOrEqualToTest() {
        Query query = buildQueryFilterGreaterThanOrEqualTo();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField >= 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanTest() {
        Query query = buildQueryFilterLessThan();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField < 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanOrEqualToTest() {
        Query query = buildQueryFilterLessThanOrEqualTo();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField <= 12.34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterContainsTest() {
        Query query = buildQueryFilterContains();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField REGEXP '.*testFilterValue.*'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotContainsTest() {
        Query query = buildQueryFilterNotContains();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField NOT REGEXP '.*testFilterValue.*'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterAndTest() {
        Query query = buildQueryFilterAnd();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testDatabase.testTable.testFilterField1 = 'testFilterValue1' AND " +
            "testDatabase.testTable.testFilterField2 = 'testFilterValue2')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterOrTest() {
        Query query = buildQueryFilterOr();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testDatabase.testTable.testFilterField1 = 'testFilterValue1' OR " +
            "testDatabase.testTable.testFilterField2 = 'testFilterValue2')";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAvgTest() {
        Query query = buildQueryAggregateAvg();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testDatabase.testTable.testAggField) AS testAggLabel FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountAllTest() {
        Query query = buildQueryAggregateCountAll();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(*) AS testAggLabel FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldTest() {
        Query query = buildQueryAggregateCountField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(testDatabase.testTable.testAggField) AS testAggLabel FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMaxTest() {
        Query query = buildQueryAggregateMax();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MAX(testDatabase.testTable.testAggField) AS testAggLabel FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMinTest() {
        Query query = buildQueryAggregateMin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MIN(testDatabase.testTable.testAggField) AS testAggLabel FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateSumTest() {
        Query query = buildQueryAggregateSum();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testDatabase.testTable.testAggField) AS testAggLabel FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateTest() {
        Query query = buildQueryMultipleAggregate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testDatabase.testTable.testAggField1) AS testAggLabel1, " +
            "SUM(testDatabase.testTable.testAggField2) AS testAggLabel2 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFieldTest() {
        Query query = buildQueryGroupByField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testDatabase.testTable.testGroupField FROM testDatabase.testTable GROUP BY testDatabase.testTable.testGroupField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateSecondTest() {
        Query query = buildQueryGroupByDateSecond();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SECOND(testDatabase.testTable.testGroupField) AS testGroupLabel FROM testDatabase.testTable GROUP BY testGroupLabel";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMinuteTest() {
        Query query = buildQueryGroupByDateMinute();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MINUTE(testDatabase.testTable.testGroupField) AS testGroupLabel FROM testDatabase.testTable GROUP BY testGroupLabel";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateHourTest() {
        Query query = buildQueryGroupByDateHour();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT HOUR(testDatabase.testTable.testGroupField) AS testGroupLabel FROM testDatabase.testTable GROUP BY testGroupLabel";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateDayTest() {
        Query query = buildQueryGroupByDateDay();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT DAYOFMONTH(testDatabase.testTable.testGroupField) AS testGroupLabel FROM testDatabase.testTable GROUP BY testGroupLabel";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMonthTest() {
        Query query = buildQueryGroupByDateMonth();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MONTH(testDatabase.testTable.testGroupField) AS testGroupLabel FROM testDatabase.testTable GROUP BY testGroupLabel";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateYearTest() {
        Query query = buildQueryGroupByDateYear();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT YEAR(testDatabase.testTable.testGroupField) AS testGroupLabel FROM testDatabase.testTable GROUP BY testGroupLabel";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByFieldTest() {
        Query query = buildQueryMultipleGroupByField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testDatabase.testTable.testGroupField1, testDatabase.testTable.testGroupField2, testDatabase.testTable.testGroupField3 FROM testDatabase.testTable " +
            "GROUP BY testDatabase.testTable.testGroupField1, testDatabase.testTable.testGroupField2, testDatabase.testTable.testGroupField3";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByDateTest() {
        Query query = buildQueryMultipleGroupByDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT MINUTE(testDatabase.testTable.testGroupField1) AS testGroupLabel1, HOUR(testDatabase.testTable.testGroupField2) AS testGroupLabel2, " +
            "DAYOFMONTH(testDatabase.testTable.testGroupField3) AS testGroupLabel3, MONTH(testDatabase.testTable.testGroupField4) AS testGroupLabel4, " +
            "YEAR(testDatabase.testTable.testGroupField5) AS testGroupLabel5 FROM testDatabase.testTable GROUP BY testGroupLabel1, " +
            "testGroupLabel2, testGroupLabel3, testGroupLabel4, testGroupLabel5";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByDateAndFieldTest() {
        Query query = buildQueryMultipleGroupByDateAndField();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testDatabase.testTable.testGroupField1, YEAR(testDatabase.testTable.testGroupField2) AS testGroupLabel2, testDatabase.testTable.testGroupField3, " +
            "MONTH(testDatabase.testTable.testGroupField4) AS testGroupLabel4 FROM testDatabase.testTable GROUP BY testDatabase.testTable.testGroupField1, " +
            "testGroupLabel2, testDatabase.testTable.testGroupField3, testGroupLabel4";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupTest() {
        Query query = buildQueryAggregateAndGroup();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testDatabase.testTable.testAggField) AS testAggLabel, testDatabase.testTable.testGroupField FROM testDatabase.testTable GROUP BY testDatabase.testTable.testGroupField";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountGroupTest() {
        Query query = buildQueryAggregateCountGroup();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT YEAR(testDatabase.testTable.testGroupField) AS testGroupLabel FROM testDatabase.testTable GROUP BY testGroupLabel";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateAndGroupTest() {
        Query query = buildQueryMultipleAggregateAndGroup();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testDatabase.testTable.testAggField1) AS testAggLabel1, " +
            "SUM(testDatabase.testTable.testAggField2) AS testAggLabel2, testDatabase.testTable.testGroupField1, " +
            "YEAR(testDatabase.testTable.testGroupField2) AS testGroupLabel2, testDatabase.testTable.testGroupField3, " +
            "MONTH(testDatabase.testTable.testGroupField4) AS testGroupLabel4 FROM testDatabase.testTable GROUP BY " +
            "testDatabase.testTable.testGroupField1, testGroupLabel2, testDatabase.testTable.testGroupField3, " +
            "testGroupLabel4";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupByDateTest() {
        Query query = buildQueryAggregateAndGroupByDate();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testDatabase.testTable.testAggField) AS testAggLabel, YEAR(testDatabase.testTable.testGroupField1) AS testGroupLabel1, " +
            "MONTH(testDatabase.testTable.testGroupField2) AS testGroupLabel2 FROM testDatabase.testTable GROUP BY testGroupLabel1, " +
            "testGroupLabel2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortAscendingTest() {
        Query query = buildQuerySortAscending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable ORDER BY testDatabase.testTable.testSortField ASC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortDescendingTest() {
        Query query = buildQuerySortDescending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable ORDER BY testDatabase.testTable.testSortField DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortOnAggregationAscendingTest() {
        Query query = buildQuerySortOnAggregationAscending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testDatabase.testTable.testField) AS testAggLabel, " +
            "testDatabase.testTable.testField FROM testDatabase.testTable GROUP BY testDatabase.testTable.testField " +
            "ORDER BY testAggLabel ASC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortOnAggregationDescendingTest() {
        Query query = buildQuerySortOnAggregationDescending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT SUM(testDatabase.testTable.testField) AS testAggLabel, " +
            "testDatabase.testTable.testField FROM testDatabase.testTable GROUP BY testDatabase.testTable.testField " +
            "ORDER BY testAggLabel DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortOnGroupOperationAscendingTest() {
        Query query = buildQuerySortOnGroupOperationAscending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT YEAR(testDatabase.testTable.testGroupField) AS testGroupLabel FROM " +
            "testDatabase.testTable GROUP BY testGroupLabel ORDER BY testGroupLabel ASC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortOnGroupOperationDescendingTest() {
        Query query = buildQuerySortOnGroupOperationDescending();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT YEAR(testDatabase.testTable.testGroupField) AS testGroupLabel FROM " +
            "testDatabase.testTable GROUP BY testGroupLabel ORDER BY testGroupLabel DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleSortTest() {
        Query query = buildQueryMultipleSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable ORDER BY testDatabase.testTable.testSortField1 ASC, testDatabase.testTable.testSortField2 DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupAndSortTest() {
        Query query = buildQueryAggregateAndGroupAndSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testDatabase.testTable.testField1) AS testAggLabel1, SUM(testDatabase.testTable.testField2) AS testAggLabel2, testDatabase.testTable.testField1, " +
            "testDatabase.testTable.testField2 FROM testDatabase.testTable GROUP BY testDatabase.testTable.testField1, testDatabase.testTable.testField2 ORDER BY testDatabase.testTable.testField1 ASC, " +
            "testDatabase.testTable.testField2 DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndSortTest() {
        Query query = buildQueryAggregateAndSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT AVG(testDatabase.testTable.testField1) AS testAggLabel1, SUM(testDatabase.testTable.testField2) AS testAggLabel2 FROM " +
            "testDatabase.testTable ORDER BY testDatabase.testTable.testField1 ASC, testDatabase.testTable.testField3 DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFieldAndSortTest() {
        Query query = buildQueryGroupByFieldAndSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT testDatabase.testTable.testField1, testDatabase.testTable.testField2 FROM testDatabase.testTable GROUP BY " +
            "testDatabase.testTable.testField1, testDatabase.testTable.testField2 ORDER BY testDatabase.testTable.testField1 ASC, testDatabase.testTable.testField3 DESC";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFunctionAndSortTest() {
        Query query = buildQueryGroupByFunctionAndSort();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT YEAR(testDatabase.testTable.testGroupField1) AS testGroupLabel1, MONTH(testDatabase.testTable.testGroupField2) AS testGroupLabel2 " +
            "FROM testDatabase.testTable GROUP BY testGroupLabel1, testGroupLabel2 ORDER BY testGroupLabel1 ASC, testGroupLabel2 DESC";
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
        String expected = "SELECT testDatabase.testTable.testField1, testDatabase.testTable.testField2, SUM(testDatabase.testTable.testAggField) AS testAggLabel, testDatabase.testTable.testGroupField FROM " +
            "testDatabase.testTable WHERE testDatabase.testTable.testFilterField = 'testFilterValue' GROUP BY testDatabase.testTable.testGroupField ORDER BY " +
            "testDatabase.testTable.testSortField ASC LIMIT 12 OFFSET 34";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryDuplicateFieldsTest() {
        Query query = buildQueryDuplicateFields();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.POSTGRESQL);
        String expected = "SELECT testDatabase.testTable.testField1, testDatabase.testTable.testField2, testDatabase.testTable.testField3, SUM(testDatabase.testTable.testField1) AS testAggLabel FROM " +
            "testDatabase.testTable GROUP BY testDatabase.testTable.testField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterDateRangeTest() {
        Query query = buildQueryFilterDateRange();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testDatabase.testTable.testFilterField >= " +
            "STR_TO_DATE('2018-01-01T00:00:00Z','%Y-%m-%dT%TZ') AND testDatabase.testTable.testFilterField <= " +
            "STR_TO_DATE('2019-01-01T00:00:00Z','%Y-%m-%dT%TZ'))";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNumberRangeTest() {
        Query query = buildQueryFilterNumberRange();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE (testDatabase.testTable.testFilterField >= 12.34 AND testDatabase.testTable.testFilterField <= 56.78)";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNestedAndTest() {
        Query query = buildQueryFilterNestedAnd();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE ((testDatabase.testTable.testFilterField1 = 'testFilterValue1' OR " +
            "testDatabase.testTable.testFilterField2 = 'testFilterValue2') AND (testDatabase.testTable.testFilterField3 = 'testFilterValue3' OR " +
            "testDatabase.testTable.testFilterField4 = 'testFilterValue4'))";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNestedOrTest() {
        Query query = buildQueryFilterNestedOr();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE ((testDatabase.testTable.testFilterField1 = 'testFilterValue1' AND " +
            "testDatabase.testTable.testFilterField2 = 'testFilterValue2') OR (testDatabase.testTable.testFilterField3 = 'testFilterValue3' AND " +
            "testDatabase.testTable.testFilterField4 = 'testFilterValue4'))";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldAndFilterTest() {
        Query query = buildQueryAggregateCountFieldAndFilter();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(testDatabase.testTable.testAggField) AS testAggLabel FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField = 'testFilterValue'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldAndMetricsTest() {
        Query query = buildQueryAggregateCountFieldAndMetrics();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT COUNT(testDatabase.testTable.testAggField) AS testAggLabel1, SUM(testDatabase.testTable.testAggField) AS testAggLabel2 FROM testDatabase.testTable";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterContainsQuotesTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "contains", "'; SQL INJECTION; '"));
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField REGEXP '.*\\'; SQL INJECTION; \\'.*'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsQuotesTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", "'; SQL INJECTION; '"));
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTable WHERE testDatabase.testTable.testFilterField = '\\'; SQL INJECTION; \\''";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryJoinTest() {
        Query query = buildQueryJoin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTableA JOIN testDatabase.testTableB ON " +
            "testDatabase.testTableA.testField1 = testDatabase.testTableB.testField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryCrossJoinTest() {
        Query query = buildQueryCrossJoin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTableA CROSS JOIN testDatabase.testTableB ON " +
            "testDatabase.testTableA.testField1 = testDatabase.testTableB.testField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFullJoinTest() {
        Query query = buildQueryFullJoin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        // MySQL does not support full joins.
        String expected = null;
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryInnerJoinTest() {
        Query query = buildQueryInnerJoin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTableA INNER JOIN testDatabase.testTableB ON " +
            "testDatabase.testTableA.testField1 = testDatabase.testTableB.testField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLeftJoinTest() {
        Query query = buildQueryLeftJoin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTableA LEFT JOIN testDatabase.testTableB ON " +
            "testDatabase.testTableA.testField1 = testDatabase.testTableB.testField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryRightJoinTest() {
        Query query = buildQueryRightJoin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTableA RIGHT JOIN testDatabase.testTableB ON " +
            "testDatabase.testTableA.testField1 = testDatabase.testTableB.testField2";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryJoinWithCompoundOnTest() {
        Query query = buildQueryJoinWithCompoundOn();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTableA JOIN testDatabase.testTableB ON " +
            "(testDatabase.testTableA.testField1 = testDatabase.testTableB.testField2 AND " +
            "testDatabase.testTableA.testField3 != 0.0 AND testDatabase.testTableB.testField4 = 'a' AND " +
            "(testDatabase.testTableA.testField5 > STR_TO_DATE('2019-01-01T00:00:00Z','%Y-%m-%dT%TZ') OR " +
            "testDatabase.testTableB.testField6 IS NOT NULL))";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleJoinTest() {
        Query query = buildQueryMultipleJoin();
        String actual = SqlQueryConverter.convertQuery(query, SqlType.MYSQL);
        String expected = "SELECT * FROM testDatabase.testTableA JOIN testDatabase.testTableB ON " +
            "testDatabase.testTableA.testField1 = testDatabase.testTableB.testField2 JOIN testDatabase.testTableC " +
            "ON testDatabase.testTableA.testField1 = testDatabase.testTableC.testField3";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertMutationQueryByIdTest() {
        MutateQuery mutateQuery = buildMutationByIdQuery();
        String actual = SqlQueryConverter.convertMutationQuery(mutateQuery);
        String expected = "UPDATE testDatabase.testTable SET testString = 'a', testZero = 0, testInteger = 1, " +
            "testDecimal = 0.5, testNegativeInteger = -1, testNegativeDecimal = -0.5, testTrue = true, " +
            "testFalse = false WHERE testIdField = 'testId'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertArrayAndObjectMutationQueryByIdTest() {
        MutateQuery mutateQuery = buildArrayAndObjectMutationByIdQuery();
        String actual = SqlQueryConverter.convertMutationQuery(mutateQuery);
        String expected = "UPDATE testDatabase.testTable SET testEmptyArray = '[]', testEmptyObject = '{}', " +
            "testArray = '[\"b\",2,true,{\"testArrayObjectString\":\"c\",\"testArrayObjectInteger\":3}]', " +
            "testObject = '{\"testObjectString\":\"d\",\"testObjectInteger\":4,\"testObjectBoolean\":true," +
            "\"testObjectArray\":[\"e\",5]}' WHERE testIdField = 'testId'";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertMutationQueryDeleteTest() {
        MutateQuery mutateQuery = buildMutationByIdQuery();
        String actual = SqlQueryConverter.convertMutationQueryIntoDeleteQuery(mutateQuery);
        String expected = "DELETE FROM WHERE testDatabase.testTable.testIdField = testId";
        assertThat(actual).isEqualTo(expected);
    }
}
