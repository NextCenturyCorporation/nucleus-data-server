package com.ncc.neon.server.services.adapter.es;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.AndWhereClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.GroupByFunctionClause;
import com.ncc.neon.server.models.query.clauses.LimitClause;
import com.ncc.neon.server.models.query.clauses.OffsetClause;
import com.ncc.neon.server.models.query.clauses.OrWhereClause;
import com.ncc.neon.server.models.query.clauses.SingularWhereClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.clauses.SortOrder;
import com.ncc.neon.server.models.query.filter.Filter;
import com.ncc.neon.util.DateUtil;

import org.elasticsearch.action.search.SearchRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=ElasticSearchRestConversionStrategy.class)
public class ElasticSearchRestConversionStrategyTest {

    @Autowired
    private ElasticSearchRestConversionStrategy strategy;

    @Test
    public void convertQueryBaseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFieldsTest() {
        Query query = new Query();
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryDistinctTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterEqualsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterNotEqualsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "!=", "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterGreaterThanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", ">", "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterLessThanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "<", "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterContainsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "contains", "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterNotContainsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "not contains", "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterAndTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterOrTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new OrWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryAggregateAvgTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggregateName", "avg", "testAggregateField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryAggregateCountAllTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggregateName", "count", "*")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryAggregateCountFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggregateName", "count", "testAggregateField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryAggregateMaxTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggregateName", "max", "testAggregateField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryAggregateMinTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggregateName", "min", "testAggregateField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryAggregateSumTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggregateName", "sum", "testAggregateField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryMultipleAggregateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggregateName1", "count", "testAggregateField1"),
            new AggregateClause("testAggregateName2", "sum", "testAggregateField2")
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryGroupByStringTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryGroupByDateMinuteTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "minute", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryGroupByDateHourTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "hour", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryGroupByDateDayTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "dayOfMonth", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryGroupByDateMonthTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "month", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryGroupByDateYearTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "year", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryMultipleGroupByTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFunctionClause("testGroupName2", "minute", "testGroupField2")
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQuerySortAscendingTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortOrder.ASCENDING)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQuerySortDescendingTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortOrder.DESCENDING)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryLimitTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(12));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryOffsetTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setOffsetClause(new OffsetClause(34));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryCombinedTest() {
        Query query = new Query();
        query.setAggregates(Arrays.asList(new AggregateClause("testAggregateName", "count", "testAggregateField")));
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "testFilterValue")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(12));
        query.setOffsetClause(new OffsetClause(34));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortOrder.ASCENDING)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryLimitZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(0));
        QueryOptions queryOptions = new QueryOptions();

        // Elasticsearch-specific test:  do not set the query limit to zero!
        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryLimitMaximumTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(1000000));
        QueryOptions queryOptions = new QueryOptions();

        // Elasticsearch-specific test:  do not set the query limit to more than 10,000!
        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterBooleanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", true)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterDateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDate("testFilterField", "=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z"))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterNumberTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 10)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterOnEmptyTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterOnFalseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", false)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterOnZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 0)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterDateRangeTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDate("testFilterField", ">=", DateUtil.transformStringToDate("2018-01-01T00:00Z")),
            SingularWhereClause.fromDate("testFilterField", "<=", DateUtil.transformStringToDate("2019-01-01T00:00Z"))
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterNumberRangeTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDouble("testFilterField", ">=", 10),
            SingularWhereClause.fromDouble("testFilterField", "<=", 20)
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterNestedAndTest() {
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
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }

    @Test
    public void convertQueryFilterNestedOrTest() {
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
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest esQuery = this.strategy.convertQuery(query, queryOptions);
        assertThat(esQuery).isEqualTo(new SearchRequest());
    }
}
