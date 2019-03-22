package com.ncc.neon.server.adapters.es;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import com.ncc.neon.server.models.queries.AggregateClause;
import com.ncc.neon.server.models.queries.AndWhereClause;
import com.ncc.neon.server.models.queries.Filter;
import com.ncc.neon.server.models.queries.GroupByFieldClause;
import com.ncc.neon.server.models.queries.GroupByFunctionClause;
import com.ncc.neon.server.models.queries.LimitClause;
import com.ncc.neon.server.models.queries.OffsetClause;
import com.ncc.neon.server.models.queries.OrWhereClause;
import com.ncc.neon.server.models.queries.Query;
import com.ncc.neon.server.models.queries.SingularWhereClause;
import com.ncc.neon.server.models.queries.SortClause;
import com.ncc.neon.server.models.queries.SortClauseOrder;
import com.ncc.neon.util.DateUtil;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=ElasticsearchQueryConverter.class)
public class ElasticsearchQueryConverterTest {

    private SearchSourceBuilder createSourceBuilder() {
        return createSourceBuilder(0, 10000);
    }

    private SearchSourceBuilder createSourceBuilder(int from, int size) {
        return new SearchSourceBuilder().explain(false).from(from).size(size);
    }

    private SearchRequest createRequest(String database, String table, SearchSourceBuilder source) {
        return new SearchRequest().searchType(SearchType.DFS_QUERY_THEN_FETCH).indices(database).types(table).source(source);
    }

    @Test
    public void convertQueryBaseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFieldsTest() {
        Query query = new Query();
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable"));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().fetchSource(new String[]{ "testField1", "testField2" }, null);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryDistinctTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testField1"));
        query.setFilter(new Filter("testDatabase", "testTable"));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("distinct").field("testField1").size(10000);
        SearchSourceBuilder source = createSourceBuilder().fetchSource(new String[]{ "testField1" }, null).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsBooleanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", true)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", true));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsDateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDate("testFilterField", "=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z"))));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "2019-01-01T00:00:00Z"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsEmptyTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", ""));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsFalseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", false)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", false));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNullTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromNull("testFilterField", "=")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.existsQuery("testFilterField")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNumberTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 12.34)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", 12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsStringTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=",
            "testFilterValue")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "testFilterValue"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 0)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", 0.0));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsBooleanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "!=", true)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", true)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsDateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDate("testFilterField", "!=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z"))));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "2019-01-01T00:00:00Z")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsEmptyTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "!=", "")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsFalseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "!=", false)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", false)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNullTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromNull("testFilterField", "!=")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testFilterField"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNumberTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "!=", 12.34)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", 12.34)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsStringTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "!=",
            "testFilterValue")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "testFilterValue")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "!=", 0)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", 0.0)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", ">", 12.34)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").gt(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanOrEqualToTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", ">=", 12.34)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").gte(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "<", 12.34)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").lt(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanOrEqualToTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "<=", 12.34)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").lte(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterContainsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "contains",
            "testFilterValue")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(
            QueryBuilders.regexpQuery("testFilterField", ".*testFilterValue.*"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotContainsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "not contains",
            "testFilterValue")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.regexpQuery("testFilterField", ".*testFilterValue.*")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterAndTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("testFilterField1", "testFilterValue1"))
            .must(QueryBuilders.termQuery("testFilterField2", "testFilterValue2")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterOrTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new OrWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("testFilterField1", "testFilterValue1"))
            .should(QueryBuilders.termQuery("testFilterField2", "testFilterValue2")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAvgTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "avg", "testAggField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountAllTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "*")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "testAggField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testAggField"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMaxTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "max", "testAggField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMinTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "min", "testAggField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateSumTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "avg", "testAggField1"),
            new AggregateClause("testAggName2", "sum", "testAggField2")
        ));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder1 = AggregationBuilders.stats("_statsFor_testAggField1").field("testAggField1");
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testAggField2").field("testAggField2");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1).aggregation(aggBuilder2);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("testGroupField").field("testGroupField").size(10000);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMinuteTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "minute", "testGroupField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.MINUTE).format("m");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateHourTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "hour", "testGroupField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.HOUR).format("H");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateDayTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "dayOfMonth", "testGroupField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.DAY).format("d");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMonthTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "month", "testGroupField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateYearTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "year", "testGroupField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFieldClause("testGroupField2", "Test Group Field 2"),
            new GroupByFieldClause("testGroupField3", "Test Group Field 3")
        ));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        TermsAggregationBuilder aggBuilder3 = AggregationBuilders.terms("testGroupField3").field("testGroupField3").size(10000);
        TermsAggregationBuilder aggBuilder2 = AggregationBuilders.terms("testGroupField2").field("testGroupField2").size(10000)
            .subAggregation(aggBuilder3);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField1").field("testGroupField1").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByDateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFunctionClause("testGroupName1", "minute", "testGroupField1"),
            new GroupByFunctionClause("testGroupName2", "hour", "testGroupField2"),
            new GroupByFunctionClause("testGroupName3", "dayOfMonth", "testGroupField3"),
            new GroupByFunctionClause("testGroupName4", "month", "testGroupField4"),
            new GroupByFunctionClause("testGroupName5", "year", "testGroupField5")
        ));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder5 = AggregationBuilders.dateHistogram("testGroupName5").field("testGroupField5")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy");
        DateHistogramAggregationBuilder aggBuilder4 = AggregationBuilders.dateHistogram("testGroupName4").field("testGroupField4")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M").subAggregation(aggBuilder5);
        DateHistogramAggregationBuilder aggBuilder3 = AggregationBuilders.dateHistogram("testGroupName3").field("testGroupField3")
            .dateHistogramInterval(DateHistogramInterval.DAY).format("d").subAggregation(aggBuilder4);
        DateHistogramAggregationBuilder aggBuilder2 = AggregationBuilders.dateHistogram("testGroupName2").field("testGroupField2")
            .dateHistogramInterval(DateHistogramInterval.HOUR).format("H").subAggregation(aggBuilder3);
        DateHistogramAggregationBuilder aggBuilder1 = AggregationBuilders.dateHistogram("testGroupName1").field("testGroupField1")
            .dateHistogramInterval(DateHistogramInterval.MINUTE).format("m").subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByDateAndFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFunctionClause("testGroupName2", "year", "testGroupField2"),
            new GroupByFieldClause("testGroupField3", "Test Group Field 3"),
            new GroupByFunctionClause("testGroupName4", "month", "testGroupField4")
        ));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder4 = AggregationBuilders.dateHistogram("testGroupName4").field("testGroupField4")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M");
        TermsAggregationBuilder aggBuilder3 = AggregationBuilders.terms("testGroupField3").field("testGroupField3").size(10000)
            .subAggregation(aggBuilder4);
        DateHistogramAggregationBuilder aggBuilder2 = AggregationBuilders.dateHistogram("testGroupName2").field("testGroupField2")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy").subAggregation(aggBuilder3);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField1").field("testGroupField1").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField").field("testGroupField").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateAndGroupTest() {
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

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder4 = AggregationBuilders.stats("_statsFor_testAggField2").field("testAggField2");
        StatsAggregationBuilder aggBuilder3 = AggregationBuilders.stats("_statsFor_testAggField1").field("testAggField1");
        TermsAggregationBuilder aggBuilder2 = AggregationBuilders.terms("testGroupField2").field("testGroupField2").size(10000)
            .subAggregation(aggBuilder3).subAggregation(aggBuilder4);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField1").field("testGroupField1").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortAscendingTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortClauseOrder.ASCENDING)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField").order(SortOrder.ASC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortDescendingTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", SortClauseOrder.DESCENDING)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField").order(SortOrder.DESC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleSortTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(
            new SortClause("testSortField1", SortClauseOrder.ASCENDING),
            new SortClause("testSortField2", SortClauseOrder.DESCENDING)
        ));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField1").order(SortOrder.ASC))
            .sort(SortBuilders.fieldSort("testSortField2").order(SortOrder.DESC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupAndSortTest() {
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

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder4 = AggregationBuilders.stats("_statsFor_testField2").field("testField2");
        StatsAggregationBuilder aggBuilder3 = AggregationBuilders.stats("_statsFor_testField1").field("testField1");
        TermsAggregationBuilder aggBuilder2 = AggregationBuilders.terms("testField2").field("testField2").size(10000)
            .order(BucketOrder.key(false)).subAggregation(aggBuilder3).subAggregation(aggBuilder4);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField1").field("testField1").size(10000)
            .order(BucketOrder.key(true)).subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndSortTest() {
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

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder1 = AggregationBuilders.stats("_statsFor_testField1").field("testField1");
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testField2").field("testField2");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1).aggregation(aggBuilder2)
            .sort(SortBuilders.fieldSort("testField1").order(SortOrder.ASC))
            .sort(SortBuilders.fieldSort("testField2").order(SortOrder.DESC))
            .sort(SortBuilders.fieldSort("testField4").order(SortOrder.ASC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupAndSortTest() {
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

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        TermsAggregationBuilder aggBuilder3 = AggregationBuilders.terms("testField3").field("testField3").size(10000);
        TermsAggregationBuilder aggBuilder2 = AggregationBuilders.terms("testField2").field("testField2").size(10000)
            .order(BucketOrder.key(false)).subAggregation(aggBuilder3);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField1").field("testField1").size(10000)
            .order(BucketOrder.key(true)).subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(12));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder(0, 12);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryOffsetTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setOffsetClause(new OffsetClause(34));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder(34, 9966);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryCombinedTest() {
        Query query = new Query();
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testField1")));
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testField1", "=", "testValue")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testField1", "Test Field 1")));
        query.setLimitClause(new LimitClause(12));
        query.setOffsetClause(new OffsetClause(34));
        query.setSortClauses(Arrays.asList(new SortClause("testField1", SortClauseOrder.ASCENDING)));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testField1", "testValue"));
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testField1").field("testField1");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField1").field("testField1").size(12)
            .order(BucketOrder.key(true)).subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(34, 12).fetchSource(new String[]{ "testField1", "testField2" }, null)
            .query(queryBuilder).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(0));

        // Elasticsearch-specific test:  do not set the query limit to zero!
        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitMaximumTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(1000000));

        // Elasticsearch-specific test:  do not set the query limit to more than 10,000!
        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        expected = expected.scroll(TimeValue.timeValueMinutes(1));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterDateRangeTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDate("testFilterField", ">=", DateUtil.transformStringToDate("2018-01-01T00:00Z")),
            SingularWhereClause.fromDate("testFilterField", "<=", DateUtil.transformStringToDate("2019-01-01T00:00Z"))
        ))));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("testFilterField").gte("2018-01-01T00:00:00Z"))
            .must(QueryBuilders.rangeQuery("testFilterField").lte("2019-01-01T00:00:00Z")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNumberRangeTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDouble("testFilterField", ">=", 12.34),
            SingularWhereClause.fromDouble("testFilterField", "<=", 56.78)
        ))));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("testFilterField").gte(12.34))
            .must(QueryBuilders.rangeQuery("testFilterField").lte(56.78)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
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

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilderA = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("testFilterField1", "testFilterValue1"))
            .should(QueryBuilders.termQuery("testFilterField2", "testFilterValue2"));
        BoolQueryBuilder queryBuilderB = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("testFilterField3", "testFilterValue3"))
            .should(QueryBuilders.termQuery("testFilterField4", "testFilterValue4"));
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().must(queryBuilderA).must(queryBuilderB));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
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

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilderA = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("testFilterField1", "testFilterValue1"))
            .must(QueryBuilders.termQuery("testFilterField2", "testFilterValue2"));
        BoolQueryBuilder queryBuilderB = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("testFilterField3", "testFilterValue3"))
            .must(QueryBuilders.termQuery("testFilterField4", "testFilterValue4"));
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().should(queryBuilderA)
            .should(queryBuilderB));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldAndFilterTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=",
            "testFilterValue")));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "testAggField")));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "testFilterValue"))
            .must(QueryBuilders.existsQuery("testAggField"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldAndMetricsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "count", "testAggField"),
            new AggregateClause("testAggName2", "sum", "testAggField")
        ));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testAggField"));
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }
}
