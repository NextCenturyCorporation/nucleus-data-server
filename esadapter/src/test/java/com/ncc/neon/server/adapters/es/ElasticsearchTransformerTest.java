package com.ncc.neon.server.adapters.es;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
import com.ncc.neon.server.models.query.filter.Filter;
import com.ncc.neon.server.models.query.result.TabularQueryResult;
import com.ncc.neon.util.DateUtil;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=ElasticsearchTransformer.class)
public class ElasticsearchTransformerTest {

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
    public void transformQueryBaseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFieldsTest() {
        Query query = new Query();
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder().fetchSource(new String[]{ "testField1", "testField2" }, null);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryDistinctTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testField1"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("distinct").field("testField1").size(10000);
        SearchSourceBuilder source = createSourceBuilder().fetchSource(new String[]{ "testField1" }, null).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsBooleanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", true)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", true));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsDateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDate("testFilterField", "=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z"))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "2019-01-01T00:00:00Z"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsEmptyTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", ""));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsFalseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "=", false)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", false));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsNullTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromNull("testFilterField", "=")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.existsQuery("testFilterField")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsNumberTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 12.34)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", 12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsStringTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=",
            "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "testFilterValue"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterEqualsZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "=", 0)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", 0.0));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsBooleanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "!=", true)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", true)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsDateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDate("testFilterField", "!=",
            DateUtil.transformStringToDate("2019-01-01T00:00Z"))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "2019-01-01T00:00:00Z")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsEmptyTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "!=", "")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsFalseTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromBoolean("testFilterField", "!=", false)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", false)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsNullTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromNull("testFilterField", "!=")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testFilterField"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsNumberTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "!=", 12.34)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", 12.34)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsStringTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "!=",
            "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "testFilterValue")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotEqualsZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "!=", 0)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", 0.0)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterGreaterThanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", ">", 12.34)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").gt(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterGreaterThanOrEqualToTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", ">=", 12.34)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").gte(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterLessThanTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "<", 12.34)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").lt(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterLessThanOrEqualToTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromDouble("testFilterField", "<=", 12.34)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").lte(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterContainsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "contains",
            "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(
            QueryBuilders.regexpQuery("testFilterField", ".*testFilterValue.*"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNotContainsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "not contains",
            "testFilterValue")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.regexpQuery("testFilterField", ".*testFilterValue.*")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterAndTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("testFilterField1", "testFilterValue1"))
            .must(QueryBuilders.termQuery("testFilterField2", "testFilterValue2")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterOrTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new OrWhereClause(Arrays.asList(
            SingularWhereClause.fromString("testFilterField1", "=", "testFilterValue1"),
            SingularWhereClause.fromString("testFilterField2", "=", "testFilterValue2")
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("testFilterField1", "testFilterValue1"))
            .should(QueryBuilders.termQuery("testFilterField2", "testFilterValue2")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateAvgTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "avg", "testAggField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateCountAllTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "*")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateCountFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "testAggField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testAggField"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateMaxTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "max", "testAggField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateMinTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "min", "testAggField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateSumTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryMultipleAggregateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "avg", "testAggField1"),
            new AggregateClause("testAggName2", "sum", "testAggField2")
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        StatsAggregationBuilder aggBuilder1 = AggregationBuilders.stats("_statsFor_testAggField1").field("testAggField1");
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testAggField2").field("testAggField2");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1).aggregation(aggBuilder2);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryGroupByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("testGroupField").field("testGroupField").size(10000);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryGroupByDateMinuteTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "minute", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.MINUTE).format("m");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryGroupByDateHourTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "hour", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.HOUR).format("H");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryGroupByDateDayTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "dayOfMonth", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.DAY).format("d");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryGroupByDateMonthTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "month", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryGroupByDateYearTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testGroupName", "year", "testGroupField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupName").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryMultipleGroupByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFieldClause("testGroupField2", "Test Group Field 2"),
            new GroupByFieldClause("testGroupField3", "Test Group Field 3")
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryMultipleGroupByDateTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFunctionClause("testGroupName1", "minute", "testGroupField1"),
            new GroupByFunctionClause("testGroupName2", "hour", "testGroupField2"),
            new GroupByFunctionClause("testGroupName3", "dayOfMonth", "testGroupField3"),
            new GroupByFunctionClause("testGroupName4", "month", "testGroupField4"),
            new GroupByFunctionClause("testGroupName5", "year", "testGroupField5")
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryMultipleGroupByDateAndFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField1", "Test Group Field 1"),
            new GroupByFunctionClause("testGroupName2", "year", "testGroupField2"),
            new GroupByFieldClause("testGroupField3", "Test Group Field 3"),
            new GroupByFunctionClause("testGroupName4", "month", "testGroupField4")
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryAggregateAndGroupTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField").field("testGroupField").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryMultipleAggregateAndGroupTest() {
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
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQuerySortAscendingTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField").order(SortOrder.ASC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQuerySortDescendingTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testSortField", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField").order(SortOrder.DESC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryMultipleSortTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(
            new SortClause("testSortField1", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING),
            new SortClause("testSortField2", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING)
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField1").order(SortOrder.ASC))
            .sort(SortBuilders.fieldSort("testSortField2").order(SortOrder.DESC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateAndGroupAndSortTest() {
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
            new SortClause("testField1", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING),
            new SortClause("testField2", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING),
            new SortClause("testField4", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryAggregateAndSortTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "avg", "testField1"),
            new AggregateClause("testAggName2", "sum", "testField2")
        ));
        query.setSortClauses(Arrays.asList(
            new SortClause("testField1", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING),
            new SortClause("testField2", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING),
            new SortClause("testField4", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryGroupAndSortTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testField1", "Test Field 1"),
            new GroupByFieldClause("testField2", "Test Field 2"),
            new GroupByFieldClause("testField3", "Test Field 3")
        ));
        query.setSortClauses(Arrays.asList(
            new SortClause("testField1", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING),
            new SortClause("testField2", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING),
            new SortClause("testField4", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryLimitTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(12));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder(0, 12);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryOffsetTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setOffsetClause(new OffsetClause(34));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder(34, 9966);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryCombinedTest() {
        Query query = new Query();
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "sum", "testField1")));
        query.setFields(Arrays.asList("testField1", "testField2"));
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testField1", "=", "testValue")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testField1", "Test Field 1")));
        query.setLimitClause(new LimitClause(12));
        query.setOffsetClause(new OffsetClause(34));
        query.setSortClauses(Arrays.asList(new SortClause("testField1", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryLimitZeroTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(0));
        QueryOptions queryOptions = new QueryOptions();

        // Elasticsearch-specific test:  do not set the query limit to zero!
        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryLimitMaximumTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(1000000));
        QueryOptions queryOptions = new QueryOptions();

        // Elasticsearch-specific test:  do not set the query limit to more than 10,000!
        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        expected = expected.scroll(TimeValue.timeValueMinutes(1));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterDateRangeTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDate("testFilterField", ">=", DateUtil.transformStringToDate("2018-01-01T00:00Z")),
            SingularWhereClause.fromDate("testFilterField", "<=", DateUtil.transformStringToDate("2019-01-01T00:00Z"))
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("testFilterField").gte("2018-01-01T00:00:00Z"))
            .must(QueryBuilders.rangeQuery("testFilterField").lte("2019-01-01T00:00:00Z")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNumberRangeTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromDouble("testFilterField", ">=", 12.34),
            SingularWhereClause.fromDouble("testFilterField", "<=", 56.78)
        ))));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("testFilterField").gte(12.34))
            .must(QueryBuilders.rangeQuery("testFilterField").lte(56.78)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryFilterNestedAndTest() {
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

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryFilterNestedOrTest() {
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

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
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
    public void transformQueryAggregateCountFieldAndFilterTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=",
            "testFilterValue")));
        query.setAggregates(Arrays.asList(new AggregateClause("testAggName", "count", "testAggField")));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "testFilterValue"))
            .must(QueryBuilders.existsQuery("testAggField"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformQueryAggregateCountFieldAndMetricsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "count", "testAggField"),
            new AggregateClause("testAggName2", "sum", "testAggField")
        ));
        QueryOptions queryOptions = new QueryOptions();

        SearchRequest actual = ElasticsearchTransformer.transformQuery(query, queryOptions);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testAggField"));
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void transformResultsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions options = new QueryOptions();

        SearchHit hit1 = mock(SearchHit.class);
        when(hit1.getId()).thenReturn("testId1");
        when(hit1.getSourceAsMap()).thenReturn(Map.ofEntries(
            Map.entry("testFieldA", "testValue1"),
            Map.entry("testFieldB", "testValue2")
        ));
        SearchHit hit2 = mock(SearchHit.class);
        when(hit2.getId()).thenReturn("testId2");
        when(hit2.getSourceAsMap()).thenReturn(Map.ofEntries(
            Map.entry("testFieldA", "testValue3"),
            Map.entry("testFieldB", "testValue4")
        ));
        SearchHit[] hitArray = { hit1, hit2 };
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(null);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", "testValue1"),
                Map.entry("testFieldB", "testValue2"),
                Map.entry("_id", "testId1")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue3"),
                Map.entry("testFieldB", "testValue4"),
                Map.entry("_id", "testId2")
            )
        ));
    }

    @Test
    public void transformResultsWithFilterDoesNotAffectOutputTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable", null, SingularWhereClause.fromString("testFilterField", "=", "testFilterValue")));
        QueryOptions options = new QueryOptions();

        SearchHit hit1 = mock(SearchHit.class);
        when(hit1.getId()).thenReturn("testId1");
        when(hit1.getSourceAsMap()).thenReturn(Map.ofEntries(
            Map.entry("testFieldA", "testValue1"),
            Map.entry("testFieldB", "testValue2")
        ));
        SearchHit hit2 = mock(SearchHit.class);
        when(hit2.getId()).thenReturn("testId2");
        when(hit2.getSourceAsMap()).thenReturn(Map.ofEntries(
            Map.entry("testFieldA", "testValue3"),
            Map.entry("testFieldB", "testValue4")
        ));
        SearchHit[] hitArray = { hit1, hit2 };
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(null);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", "testValue1"),
                Map.entry("testFieldB", "testValue2"),
                Map.entry("_id", "testId1")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue3"),
                Map.entry("testFieldB", "testValue4"),
                Map.entry("_id", "testId2")
            )
        ));
    }

    @Test
    public void transformResultsTotalCountAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        QueryOptions options = new QueryOptions();

        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries());
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testCount", (long) 90)
            )
        ));
    }

    @Test
    public void transformResultsCountFieldAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testAggField")));
        QueryOptions options = new QueryOptions();

        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries());
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testCount", (long) 90)
            )
        ));
    }

    @Test
    public void transformResultsAvgAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testAvg", "avg", "testAggField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getAvg()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats)
        ));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testAvg", 12.0)
            )
        ));
    }

    @Test
    public void transformResultsMaxAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testMax", "max", "testAggField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getMax()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats)
        ));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testMax", 12.0)
            )
        ));
    }

    @Test
    public void transformResultsMinAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testMin", "min", "testAggField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getMin()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats)
        ));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testMin", 12.0)
            )
        ));
    }

    @Test
    public void transformResultsSumAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getSum()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats)
        ));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    public void transformResultsMultipleAggregationsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "count", "*"),
            new AggregateClause("testAggName2", "avg", "testAggField"),
            new AggregateClause("testAggName3", "max", "testAggField"),
            new AggregateClause("testAggName4", "min", "testAggField"),
            new AggregateClause("testAggName5", "sum", "testAggField")
        ));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getAvg()).thenReturn(12.0);
        when(stats.getMax()).thenReturn(34.0);
        when(stats.getMin()).thenReturn(56.0);
        when(stats.getSum()).thenReturn(78.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats)
        ));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testAggName1", (long) 90),
                Map.entry("testAggName2", 12.0),
                Map.entry("testAggName3", 34.0),
                Map.entry("testAggName4", 56.0),
                Map.entry("testAggName5", 78.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByTotalCountAggregationWithSingleBucketTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testTotal", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("testGroup1");
        List termsAggregationList = Arrays.asList(bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testTotal", (long) 90)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithSingleBucketTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("testGroup1");
        List termsAggregationList = Arrays.asList(bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithMultipleBucketsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("testGroup1");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("testGroup2");
        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup2"),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithBooleanGroupsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn(false);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn(true);
        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", true),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", false),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithNumberGroupsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn(12.34);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn(0);
        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", 0),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", 12.34),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithLimitTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithLimitAndOffsetTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithLimitAndSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "c"),
                Map.entry("testCount", (long) 21)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithLimitAndSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testCount", (long) 87)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithLimitAndOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithLimitAndOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "c"),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithOffsetTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setOffsetClause(new OffsetClause(1));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "c"),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testCount", (long) 87)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "c"),
                Map.entry("testCount", (long) 21)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket2, bucket3, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "c"),
                Map.entry("testCount", (long) 21)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testCount", (long) 87)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountFieldAggregationWithSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket2, bucket3, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "c"),
                Map.entry("testCount", (long) 21)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testCount", (long) 43)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByNonCountAggregationWithSingleBucketTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("testGroup1");
        List termsAggregationList = Arrays.asList(bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByNonCountAggregationWithMultipleBucketsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("testGroup1");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2)
        ));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("testGroup2");
        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup2"),
                Map.entry("testSum", 34.0)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByNonCountAggregationWithLimitAndOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testSum", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2)
        ));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Stats stats3 = mock(Stats.class);
        when(stats3.getSum()).thenReturn(56.0);
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats3)
        ));
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Stats stats4 = mock(Stats.class);
        when(stats4.getSum()).thenReturn(78.0);
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats4)
        ));
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "d"),
                Map.entry("testSum", 34.0)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "a"),
                Map.entry("testSum", 56.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByNonCountAggregationWithLimitAndOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", com.ncc.neon.server.models.query.clauses.SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2)
        ));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Stats stats3 = mock(Stats.class);
        when(stats3.getSum()).thenReturn(56.0);
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats3)
        ));
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Stats stats4 = mock(Stats.class);
        when(stats4.getSum()).thenReturn(78.0);
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats4)
        ));
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getAggregations()).thenReturn(bucketAggregations4);
        when(bucket4.getDocCount()).thenReturn((long) 87);
        when(bucket4.getKey()).thenReturn("b");
        List termsAggregationList = Arrays.asList(bucket4, bucket3, bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "b"),
                Map.entry("testSum", 78.0)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "c"),
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsMultipleGroupsAndCountFieldAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testInnerGroupField")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testOuterGroupField", "Test Outer Group Field"),
            new GroupByFieldClause("testInnerGroupField", "Test Inner Group Field")
        ));
        QueryOptions options = new QueryOptions();

        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKey()).thenReturn("testInnerGroupA");
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1B = mock(Terms.Bucket.class);
        when(nestedBucket1B.getAggregations()).thenReturn(nestedBucketAggregations1B);
        when(nestedBucket1B.getDocCount()).thenReturn((long) 43);
        when(nestedBucket1B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList1 = Arrays.asList(nestedBucket1B, nestedBucket1A);
        Terms nestedTermsAggregation1 = mock(Terms.class);
        when(nestedTermsAggregation1.getBuckets()).thenReturn(nestedTermsAggregationList1);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asList()).thenReturn(Arrays.asList(nestedTermsAggregation1));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getKey()).thenReturn("testOuterGroup1");

        Aggregations nestedBucketAggregations2A = mock(Aggregations.class);
        when(nestedBucketAggregations2A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKey()).thenReturn("testInnerGroupA");
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2B = mock(Terms.Bucket.class);
        when(nestedBucket2B.getAggregations()).thenReturn(nestedBucketAggregations2B);
        when(nestedBucket2B.getDocCount()).thenReturn((long) 87);
        when(nestedBucket2B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList2 = Arrays.asList(nestedBucket2B, nestedBucket2A);
        Terms nestedTermsAggregation2 = mock(Terms.class);
        when(nestedTermsAggregation2.getBuckets()).thenReturn(nestedTermsAggregationList2);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asList()).thenReturn(Arrays.asList(nestedTermsAggregation2));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getKey()).thenReturn("testOuterGroup2");

        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsMultipleGroupsAndMultipleCountFieldAggregationsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testOuterCount", "count", "testOuterGroupField"),
            new AggregateClause("testInnerCount", "count", "testInnerGroupField")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testOuterGroupField", "Test Outer Group Field"),
            new GroupByFieldClause("testInnerGroupField", "Test Inner Group Field")
        ));
        QueryOptions options = new QueryOptions();

        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKey()).thenReturn("testInnerGroupA");
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1B = mock(Terms.Bucket.class);
        when(nestedBucket1B.getAggregations()).thenReturn(nestedBucketAggregations1B);
        when(nestedBucket1B.getDocCount()).thenReturn((long) 43);
        when(nestedBucket1B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList1 = Arrays.asList(nestedBucket1B, nestedBucket1A);
        Terms nestedTermsAggregation1 = mock(Terms.class);
        when(nestedTermsAggregation1.getBuckets()).thenReturn(nestedTermsAggregationList1);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asList()).thenReturn(Arrays.asList(nestedTermsAggregation1));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 123);
        when(bucket1.getKey()).thenReturn("testOuterGroup1");

        Aggregations nestedBucketAggregations2A = mock(Aggregations.class);
        when(nestedBucketAggregations2A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKey()).thenReturn("testInnerGroupA");
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2B = mock(Terms.Bucket.class);
        when(nestedBucket2B.getAggregations()).thenReturn(nestedBucketAggregations2B);
        when(nestedBucket2B.getDocCount()).thenReturn((long) 87);
        when(nestedBucket2B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList2 = Arrays.asList(nestedBucket2B, nestedBucket2A);
        Terms nestedTermsAggregation2 = mock(Terms.class);
        when(nestedTermsAggregation2.getBuckets()).thenReturn(nestedTermsAggregationList2);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asList()).thenReturn(Arrays.asList(nestedTermsAggregation2));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 456);
        when(bucket2.getKey()).thenReturn("testOuterGroup2");

        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testOuterCount", (long) 456),
                Map.entry("testInnerCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testOuterCount", (long) 456),
                Map.entry("testInnerCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testOuterCount", (long) 123),
                Map.entry("testInnerCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testOuterCount", (long) 123),
                Map.entry("testInnerCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsMultipleGroupsAndNonCountAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testOuterGroupField", "Test Outer Group Field"),
            new GroupByFieldClause("testInnerGroupField", "Test Inner Group Field")
        ));
        QueryOptions options = new QueryOptions();

        Stats stats1A = mock(Stats.class);
        when(stats1A.getSum()).thenReturn(12.0);
        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1A)
        ));
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKey()).thenReturn("testInnerGroupA");
        Stats stats1B = mock(Stats.class);
        when(stats1B.getSum()).thenReturn(34.0);
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1B)
        ));
        Terms.Bucket nestedBucket1B = mock(Terms.Bucket.class);
        when(nestedBucket1B.getAggregations()).thenReturn(nestedBucketAggregations1B);
        when(nestedBucket1B.getDocCount()).thenReturn((long) 43);
        when(nestedBucket1B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList1 = Arrays.asList(nestedBucket1B, nestedBucket1A);
        Terms nestedTermsAggregation1 = mock(Terms.class);
        when(nestedTermsAggregation1.getBuckets()).thenReturn(nestedTermsAggregationList1);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asList()).thenReturn(Arrays.asList(nestedTermsAggregation1));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getKey()).thenReturn("testOuterGroup1");

        Stats stats2A = mock(Stats.class);
        when(stats2A.getSum()).thenReturn(56.0);
        Aggregations nestedBucketAggregations2A = mock(Aggregations.class);
        when(nestedBucketAggregations2A.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2A)
        ));
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKey()).thenReturn("testInnerGroupA");
        Stats stats2B = mock(Stats.class);
        when(stats2B.getSum()).thenReturn(78.0);
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2B)
        ));
        Terms.Bucket nestedBucket2B = mock(Terms.Bucket.class);
        when(nestedBucket2B.getAggregations()).thenReturn(nestedBucketAggregations2B);
        when(nestedBucket2B.getDocCount()).thenReturn((long) 87);
        when(nestedBucket2B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList2 = Arrays.asList(nestedBucket2B, nestedBucket2A);
        Terms nestedTermsAggregation2 = mock(Terms.class);
        when(nestedTermsAggregation2.getBuckets()).thenReturn(nestedTermsAggregationList2);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asList()).thenReturn(Arrays.asList(nestedTermsAggregation2));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getKey()).thenReturn("testOuterGroup2");

        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testSum", 78.0)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testSum", 56.0)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testSum", 34.0)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsMultipleGroupsAndMultipleAggregationsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(
            new AggregateClause("testAggName1", "count", "testInnerGroupField"),
            new AggregateClause("testAggName2", "avg", "testAggField"),
            new AggregateClause("testAggName3", "max", "testAggField"),
            new AggregateClause("testAggName4", "min", "testAggField"),
            new AggregateClause("testAggName5", "sum", "testAggField")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testOuterGroupField", "Test Outer Group Field"),
            new GroupByFieldClause("testInnerGroupField", "Test Inner Group Field")
        ));
        QueryOptions options = new QueryOptions();

        Stats stats1A = mock(Stats.class);
        when(stats1A.getAvg()).thenReturn(12.12);
        when(stats1A.getMax()).thenReturn(12.34);
        when(stats1A.getMin()).thenReturn(12.56);
        when(stats1A.getSum()).thenReturn(12.78);
        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1A)
        ));
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKey()).thenReturn("testInnerGroupA");
        Stats stats1B = mock(Stats.class);
        when(stats1B.getAvg()).thenReturn(34.12);
        when(stats1B.getMax()).thenReturn(34.34);
        when(stats1B.getMin()).thenReturn(34.56);
        when(stats1B.getSum()).thenReturn(34.78);
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1B)
        ));
        Terms.Bucket nestedBucket1B = mock(Terms.Bucket.class);
        when(nestedBucket1B.getAggregations()).thenReturn(nestedBucketAggregations1B);
        when(nestedBucket1B.getDocCount()).thenReturn((long) 43);
        when(nestedBucket1B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList1 = Arrays.asList(nestedBucket1B, nestedBucket1A);
        Terms nestedTermsAggregation1 = mock(Terms.class);
        when(nestedTermsAggregation1.getBuckets()).thenReturn(nestedTermsAggregationList1);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asList()).thenReturn(Arrays.asList(nestedTermsAggregation1));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getKey()).thenReturn("testOuterGroup1");

        Stats stats2A = mock(Stats.class);
        when(stats2A.getAvg()).thenReturn(56.12);
        when(stats2A.getMax()).thenReturn(56.34);
        when(stats2A.getMin()).thenReturn(56.56);
        when(stats2A.getSum()).thenReturn(56.78);
        Aggregations nestedBucketAggregations2A = mock(Aggregations.class);
        when(nestedBucketAggregations2A.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2A)
        ));
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKey()).thenReturn("testInnerGroupA");
        Stats stats2B = mock(Stats.class);
        when(stats2B.getAvg()).thenReturn(78.12);
        when(stats2B.getMax()).thenReturn(78.34);
        when(stats2B.getMin()).thenReturn(78.56);
        when(stats2B.getSum()).thenReturn(78.78);
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2B)
        ));
        Terms.Bucket nestedBucket2B = mock(Terms.Bucket.class);
        when(nestedBucket2B.getAggregations()).thenReturn(nestedBucketAggregations2B);
        when(nestedBucket2B.getDocCount()).thenReturn((long) 87);
        when(nestedBucket2B.getKey()).thenReturn("testInnerGroupB");
        List nestedTermsAggregationList2 = Arrays.asList(nestedBucket2B, nestedBucket2A);
        Terms nestedTermsAggregation2 = mock(Terms.class);
        when(nestedTermsAggregation2.getBuckets()).thenReturn(nestedTermsAggregationList2);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asList()).thenReturn(Arrays.asList(nestedTermsAggregation2));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getKey()).thenReturn("testOuterGroup2");

        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testAggName1", (long) 87),
                Map.entry("testAggName2", 78.12),
                Map.entry("testAggName3", 78.34),
                Map.entry("testAggName4", 78.56),
                Map.entry("testAggName5", 78.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testAggName1", (long) 65),
                Map.entry("testAggName2", 56.12),
                Map.entry("testAggName3", 56.34),
                Map.entry("testAggName4", 56.56),
                Map.entry("testAggName5", 56.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testAggName1", (long) 43),
                Map.entry("testAggName2", 34.12),
                Map.entry("testAggName3", 34.34),
                Map.entry("testAggName4", 34.56),
                Map.entry("testAggName5", 34.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testAggName1", (long) 21),
                Map.entry("testAggName2", 12.12),
                Map.entry("testAggName3", 12.34),
                Map.entry("testAggName4", 12.56),
                Map.entry("testAggName5", 12.78)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDateGroupsAndCountFieldAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testYear")));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testYear", "year", "testGroupField")));
        QueryOptions options = new QueryOptions();

        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKeyAsString()).thenReturn("2018");
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKeyAsString()).thenReturn("2019");
        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testYear", (float) 2019),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testYear", (float) 2018),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDateGroupsAndNonCountAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testYear", "year", "testDateField")));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKeyAsString()).thenReturn("2018");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggField", stats2)
        ));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKeyAsString()).thenReturn("2019");
        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testYear", (float) 2019),
                Map.entry("testSum", 34.0)
            ),
            Map.ofEntries(
                Map.entry("testYear", (float) 2018),
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsMultipleDateGroupsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testYear")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFunctionClause("testMonth", "month", "testDateField"),
            new GroupByFunctionClause("testYear", "year", "testDateField")
        ));
        QueryOptions options = new QueryOptions();

        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKeyAsString()).thenReturn("2018");
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1B = mock(Terms.Bucket.class);
        when(nestedBucket1B.getAggregations()).thenReturn(nestedBucketAggregations1B);
        when(nestedBucket1B.getDocCount()).thenReturn((long) 43);
        when(nestedBucket1B.getKeyAsString()).thenReturn("2019");
        List nestedTermsAggregationList1 = Arrays.asList(nestedBucket1B, nestedBucket1A);
        Terms nestedTermsAggregation1 = mock(Terms.class);
        when(nestedTermsAggregation1.getBuckets()).thenReturn(nestedTermsAggregationList1);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asList()).thenReturn(Arrays.asList(nestedTermsAggregation1));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getKeyAsString()).thenReturn("1");

        Aggregations nestedBucketAggregations2A = mock(Aggregations.class);
        when(nestedBucketAggregations2A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKeyAsString()).thenReturn("2018");
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2B = mock(Terms.Bucket.class);
        when(nestedBucket2B.getAggregations()).thenReturn(nestedBucketAggregations2B);
        when(nestedBucket2B.getDocCount()).thenReturn((long) 87);
        when(nestedBucket2B.getKeyAsString()).thenReturn("2019");
        List nestedTermsAggregationList2 = Arrays.asList(nestedBucket2B, nestedBucket2A);
        Terms nestedTermsAggregation2 = mock(Terms.class);
        when(nestedTermsAggregation2.getBuckets()).thenReturn(nestedTermsAggregationList2);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asList()).thenReturn(Arrays.asList(nestedTermsAggregation2));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getKeyAsString()).thenReturn("2");

        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testMonth", (float) 2),
                Map.entry("testYear", (float) 2019),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testMonth", (float) 2),
                Map.entry("testYear", (float) 2018),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testMonth", (float) 1),
                Map.entry("testYear", (float) 2019),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testMonth", (float) 1),
                Map.entry("testYear", (float) 2018),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDateAndNonDateGroupsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testGroupField")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFunctionClause("testYear", "year", "testDateField"),
            new GroupByFieldClause("testGroupField", "Test Group Field")
        ));
        QueryOptions options = new QueryOptions();

        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKey()).thenReturn("testGroup1");
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1B = mock(Terms.Bucket.class);
        when(nestedBucket1B.getAggregations()).thenReturn(nestedBucketAggregations1B);
        when(nestedBucket1B.getDocCount()).thenReturn((long) 43);
        when(nestedBucket1B.getKey()).thenReturn("testGroup2");
        List nestedTermsAggregationList1 = Arrays.asList(nestedBucket1B, nestedBucket1A);
        Terms nestedTermsAggregation1 = mock(Terms.class);
        when(nestedTermsAggregation1.getBuckets()).thenReturn(nestedTermsAggregationList1);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asList()).thenReturn(Arrays.asList(nestedTermsAggregation1));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getKeyAsString()).thenReturn("2018");

        Aggregations nestedBucketAggregations2A = mock(Aggregations.class);
        when(nestedBucketAggregations2A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKey()).thenReturn("testGroup1");
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2B = mock(Terms.Bucket.class);
        when(nestedBucket2B.getAggregations()).thenReturn(nestedBucketAggregations2B);
        when(nestedBucket2B.getDocCount()).thenReturn((long) 87);
        when(nestedBucket2B.getKey()).thenReturn("testGroup2");
        List nestedTermsAggregationList2 = Arrays.asList(nestedBucket2B, nestedBucket2A);
        Terms nestedTermsAggregation2 = mock(Terms.class);
        when(nestedTermsAggregation2.getBuckets()).thenReturn(nestedTermsAggregationList2);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asList()).thenReturn(Arrays.asList(nestedTermsAggregation2));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getKeyAsString()).thenReturn("2019");

        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup2"),
                Map.entry("testYear", (float) 2019),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testYear", (float) 2019),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup2"),
                Map.entry("testYear", (float) 2018),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testYear", (float) 2018),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsNonDateAndDateGroupsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testYear")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testGroupField", "Test Group Field"),
            new GroupByFunctionClause("testYear", "year", "testDateField")
        ));
        QueryOptions options = new QueryOptions();

        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKeyAsString()).thenReturn("2018");
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket1B = mock(Terms.Bucket.class);
        when(nestedBucket1B.getAggregations()).thenReturn(nestedBucketAggregations1B);
        when(nestedBucket1B.getDocCount()).thenReturn((long) 43);
        when(nestedBucket1B.getKeyAsString()).thenReturn("2019");
        List nestedTermsAggregationList1 = Arrays.asList(nestedBucket1B, nestedBucket1A);
        Terms nestedTermsAggregation1 = mock(Terms.class);
        when(nestedTermsAggregation1.getBuckets()).thenReturn(nestedTermsAggregationList1);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asList()).thenReturn(Arrays.asList(nestedTermsAggregation1));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getKey()).thenReturn("testGroup1");

        Aggregations nestedBucketAggregations2A = mock(Aggregations.class);
        when(nestedBucketAggregations2A.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKeyAsString()).thenReturn("2018");
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries());
        Terms.Bucket nestedBucket2B = mock(Terms.Bucket.class);
        when(nestedBucket2B.getAggregations()).thenReturn(nestedBucketAggregations2B);
        when(nestedBucket2B.getDocCount()).thenReturn((long) 87);
        when(nestedBucket2B.getKeyAsString()).thenReturn("2019");
        List nestedTermsAggregationList2 = Arrays.asList(nestedBucket2B, nestedBucket2A);
        Terms nestedTermsAggregation2 = mock(Terms.class);
        when(nestedTermsAggregation2.getBuckets()).thenReturn(nestedTermsAggregationList2);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asList()).thenReturn(Arrays.asList(nestedTermsAggregation2));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getKey()).thenReturn("testGroup2");

        List termsAggregationList = Arrays.asList(bucket2, bucket1);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup2"),
                Map.entry("testYear", (float) 2019),
                Map.entry("testCount", (long) 87)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup2"),
                Map.entry("testYear", (float) 2018),
                Map.entry("testCount", (long) 65)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testYear", (float) 2019),
                Map.entry("testCount", (long) 43)
            ),
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testYear", (float) 2018),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDistinctValuesTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testFieldA", "testFieldB"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        QueryOptions options = new QueryOptions();

        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getKey()).thenReturn("testValue1");
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getKey()).thenReturn("testValue2");
        List termsAggregationList = Arrays.asList(bucket1, bucket2);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", "testValue1")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue2")
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDistinctValuesWithLimitTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testFieldA", "testFieldB"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(2));
        QueryOptions options = new QueryOptions();

        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getKey()).thenReturn("testValue1");
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getKey()).thenReturn("testValue2");
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getKey()).thenReturn("testValue3");
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getKey()).thenReturn("testValue4");
        List termsAggregationList = Arrays.asList(bucket1, bucket2, bucket3, bucket4);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", "testValue1")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue2")
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDistinctValuesWithOffsetTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testFieldA", "testFieldB"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setOffsetClause(new OffsetClause(1));
        QueryOptions options = new QueryOptions();

        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getKey()).thenReturn("testValue1");
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getKey()).thenReturn("testValue2");
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getKey()).thenReturn("testValue3");
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getKey()).thenReturn("testValue4");
        List termsAggregationList = Arrays.asList(bucket1, bucket2, bucket3, bucket4);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", "testValue2")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue3")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue4")
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDistinctValuesWithSortTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testFieldA", "testFieldB"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testFieldA", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING)));
        QueryOptions options = new QueryOptions();

        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getKey()).thenReturn("testValue1");
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getKey()).thenReturn("testValue2");
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getKey()).thenReturn("testValue3");
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getKey()).thenReturn("testValue4");
        List termsAggregationList = Arrays.asList(bucket1, bucket2, bucket3, bucket4);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", "testValue4")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue3")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue2")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue1")
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDistinctValuesWithLimitAndOffsetAndSortTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testFieldA", "testFieldB"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testFieldA", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING)));
        QueryOptions options = new QueryOptions();

        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getKey()).thenReturn("testValue1");
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getKey()).thenReturn("testValue2");
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getKey()).thenReturn("testValue3");
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getKey()).thenReturn("testValue4");
        List termsAggregationList = Arrays.asList(bucket1, bucket2, bucket3, bucket4);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", "testValue3")
            ),
            Map.ofEntries(
                Map.entry("testFieldA", "testValue2")
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDistinctValuesWithSortAndNumberDataTest() {
        Query query = new Query();
        query.setDistinct(true);
        query.setFields(Arrays.asList("testFieldA", "testFieldB"));
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setSortClauses(Arrays.asList(new SortClause("testFieldA", com.ncc.neon.server.models.query.clauses.SortOrder.DESCENDING)));
        QueryOptions options = new QueryOptions();

        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getKey()).thenReturn(-1);
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getKey()).thenReturn(0);
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getKey()).thenReturn(1);
        Terms.Bucket bucket4 = mock(Terms.Bucket.class);
        when(bucket4.getKey()).thenReturn(2);
        Terms.Bucket bucket5 = mock(Terms.Bucket.class);
        when(bucket5.getKey()).thenReturn(10);
        Terms.Bucket bucket6 = mock(Terms.Bucket.class);
        when(bucket6.getKey()).thenReturn(20);
        List termsAggregationList = Arrays.asList(bucket1, bucket2, bucket3, bucket4, bucket5, bucket6);
        Terms termsAggregation = mock(Terms.class);
        when(termsAggregation.getBuckets()).thenReturn(termsAggregationList);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asList()).thenReturn(Arrays.asList(termsAggregation));
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchTransformer.transformResults(query, options, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testFieldA", 20)
            ),
            Map.ofEntries(
                Map.entry("testFieldA", 10)
            ),
            Map.ofEntries(
                Map.entry("testFieldA", 2)
            ),
            Map.ofEntries(
                Map.entry("testFieldA", 1)
            ),
            Map.ofEntries(
                Map.entry("testFieldA", 0)
            ),
            Map.ofEntries(
                Map.entry("testFieldA", -1)
            )
        ));
    }
}
