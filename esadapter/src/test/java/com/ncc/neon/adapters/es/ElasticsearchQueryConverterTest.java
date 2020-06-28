package com.ncc.neon.adapters.es;

import com.ncc.neon.adapters.QueryBuilder;
import com.ncc.neon.models.queries.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
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

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=ElasticsearchQueryConverter.class)
public class ElasticsearchQueryConverterTest extends QueryBuilder {

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
        Query query = buildQueryBase();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFieldsTest() {
        Query query = buildQueryFields();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().fetchSource(new String[]{ "testField1", "testField2" }, null);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryDistinctTest() {
        Query query = buildQueryDistinct();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("distinct").field("testField1").size(10000);
        SearchSourceBuilder source = createSourceBuilder().fetchSource(new String[]{ "testField1" }, null).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsBooleanTest() {
        Query query = buildQueryFilterEqualsBoolean();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", true));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsDateTest() {
        Query query = buildQueryFilterEqualsDate();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "2019-01-01T00:00:00Z"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsEmptyTest() {
        Query query = buildQueryFilterEqualsEmpty();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", ""));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsFalseTest() {
        Query query = buildQueryFilterEqualsFalse();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", false));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNullTest() {
        Query query = buildQueryFilterEqualsNull();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.existsQuery("testFilterField")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsNumberTest() {
        Query query = buildQueryFilterEqualsNumber();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", 12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsStringTest() {
        Query query = buildQueryFilterEqualsString();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "testFilterValue"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterEqualsZeroTest() {
        Query query = buildQueryFilterEqualsZero();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", 0.0));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsBooleanTest() {
        Query query = buildQueryFilterNotEqualsBoolean();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", true)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsDateTest() {
        Query query = buildQueryFilterNotEqualsDate();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "2019-01-01T00:00:00Z")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsEmptyTest() {
        Query query = buildQueryFilterNotEqualsEmpty();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsFalseTest() {
        Query query = buildQueryFilterNotEqualsFalse();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", false)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNullTest() {
        Query query = buildQueryFilterNotEqualsNull();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testFilterField"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsNumberTest() {
        Query query = buildQueryFilterNotEqualsNumber();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", 12.34)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsStringTest() {
        Query query = buildQueryFilterNotEqualsString();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", "testFilterValue")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotEqualsZeroTest() {
        Query query = buildQueryFilterNotEqualsZero();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.termQuery("testFilterField", 0.0)));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanTest() {
        Query query = buildQueryFilterGreaterThan();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").gt(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterGreaterThanOrEqualToTest() {
        Query query = buildQueryFilterGreaterThanOrEqualTo();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").gte(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanTest() {
        Query query = buildQueryFilterLessThan();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").lt(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterLessThanOrEqualToTest() {
        Query query = buildQueryFilterLessThanOrEqualTo();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("testFilterField").lte(12.34));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterContainsTest() {
        Query query = buildQueryFilterContains();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(
            QueryBuilders.regexpQuery("testFilterField", ".*testFilterValue.*"));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterNotContainsTest() {
        Query query = buildQueryFilterNotContains();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().mustNot(
            QueryBuilders.regexpQuery("testFilterField", ".*testFilterValue.*")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterAndTest() {
        Query query = buildQueryFilterAnd();

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
        Query query = buildQueryFilterOr();

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
        Query query = buildQueryAggregateAvg();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountAllTest() {
        Query query = buildQueryAggregateCountAll();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder(0, 1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldTest() {
        Query query = buildQueryAggregateCountField();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testAggField"));
        SearchSourceBuilder source = createSourceBuilder(0, 1).query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMaxTest() {
        Query query = buildQueryAggregateMax();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateMinTest() {
        Query query = buildQueryAggregateMin();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateSumTest() {
        Query query = buildQueryAggregateSum();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateTest() {
        Query query = buildQueryMultipleAggregate();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder1 = AggregationBuilders.stats("_statsFor_testAggField1").field("testAggField1");
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testAggField2").field("testAggField2");
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1).aggregation(aggBuilder2);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFieldTest() {
        Query query = buildQueryGroupByField();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        TermsAggregationBuilder aggBuilder = AggregationBuilders.terms("testGroupField").field("testGroupField").size(10000);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateSecondTest() {
        Query query = buildQueryGroupByDateSecond();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.SECOND).format("s");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMinuteTest() {
        Query query = buildQueryGroupByDateMinute();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.MINUTE).format("m");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateHourTest() {
        Query query = buildQueryGroupByDateHour();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.HOUR).format("H");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateDayTest() {
        Query query = buildQueryGroupByDateDay();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.DAY).format("d");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateMonthTest() {
        Query query = buildQueryGroupByDateMonth();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByDateYearTest() {
        Query query = buildQueryGroupByDateYear();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy");
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleGroupByFieldTest() {
        Query query = buildQueryMultipleGroupByField();

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
        Query query = buildQueryMultipleGroupByDate();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder5 = AggregationBuilders.dateHistogram("testGroupLabel5").field("testGroupField5")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy");
        DateHistogramAggregationBuilder aggBuilder4 = AggregationBuilders.dateHistogram("testGroupLabel4").field("testGroupField4")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M").subAggregation(aggBuilder5);
        DateHistogramAggregationBuilder aggBuilder3 = AggregationBuilders.dateHistogram("testGroupLabel3").field("testGroupField3")
            .dateHistogramInterval(DateHistogramInterval.DAY).format("d").subAggregation(aggBuilder4);
        DateHistogramAggregationBuilder aggBuilder2 = AggregationBuilders.dateHistogram("testGroupLabel2").field("testGroupField2")
            .dateHistogramInterval(DateHistogramInterval.HOUR).format("H").subAggregation(aggBuilder3);
        DateHistogramAggregationBuilder aggBuilder1 = AggregationBuilders.dateHistogram("testGroupLabel1").field("testGroupField1")
            .dateHistogramInterval(DateHistogramInterval.MINUTE).format("m").subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertQueryMultipleGroupByDateAndFieldTest() {
        Query query = buildQueryMultipleGroupByDateAndField();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder4 = AggregationBuilders.dateHistogram("testGroupLabel4").field("testGroupField4")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M");
        TermsAggregationBuilder aggBuilder3 = AggregationBuilders.terms("testGroupField3").field("testGroupField3").size(10000)
            .subAggregation(aggBuilder4);
        DateHistogramAggregationBuilder aggBuilder2 = AggregationBuilders.dateHistogram("testGroupLabel2").field("testGroupField2")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy").subAggregation(aggBuilder3);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField1").field("testGroupField1").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertQueryAggregateAndGroupTest() {
        Query query = buildQueryAggregateAndGroup();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField").field("testGroupField").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountGroupTest() {
        Query query = buildQueryAggregateCountGroup();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy");
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleAggregateAndGroupTest() {
        Query query = buildQueryMultipleAggregateAndGroup();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder6 = AggregationBuilders.stats("_statsFor_testAggField2").field("testAggField2");
        StatsAggregationBuilder aggBuilder5 = AggregationBuilders.stats("_statsFor_testAggField1").field("testAggField1");
        DateHistogramAggregationBuilder aggBuilder4 = AggregationBuilders.dateHistogram("testGroupLabel4")
            .field("testGroupField4").dateHistogramInterval(DateHistogramInterval.MONTH).format("M")
            .subAggregation(aggBuilder5).subAggregation(aggBuilder6);
        TermsAggregationBuilder aggBuilder3 = AggregationBuilders.terms("testGroupField3").field("testGroupField3").size(10000)
            .subAggregation(aggBuilder4);
        DateHistogramAggregationBuilder aggBuilder2 = AggregationBuilders.dateHistogram("testGroupLabel2").field("testGroupField2")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy").subAggregation(aggBuilder3);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField1").field("testGroupField1").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertQueryAggregateAndGroupByDateTest() {
        Query query = buildQueryAggregateAndGroupByDate();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder3 = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        DateHistogramAggregationBuilder aggBuilder2 = AggregationBuilders.dateHistogram("testGroupLabel2").field("testGroupField2")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M").subAggregation(aggBuilder3);
        DateHistogramAggregationBuilder aggBuilder1 = AggregationBuilders.dateHistogram("testGroupLabel1").field("testGroupField1")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy").subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortAscendingTest() {
        Query query = buildQuerySortAscending();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField").order(SortOrder.ASC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortDescendingTest() {
        Query query = buildQuerySortDescending();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField").order(SortOrder.DESC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }


    @Test
    public void convertQuerySortOnAggregationAscendingTest() {
        Query query = buildQuerySortOnAggregationAscending();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testField").field("testField");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField").field("testField").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortOnAggregationDescendingTest() {
        Query query = buildQuerySortOnAggregationDescending();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testField").field("testField");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField").field("testField").size(10000)
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortOnGroupOperationAscendingTest() {
        Query query = buildQuerySortOnGroupOperationAscending();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder1 = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy").order(BucketOrder.key(true));
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortOnGroupOperationDescendingTest() {
        Query query = buildQuerySortOnGroupOperationDescending();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder1 = AggregationBuilders.dateHistogram("testGroupLabel").field("testGroupField")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy").order(BucketOrder.key(false));
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleSortTest() {
        Query query = buildQueryMultipleSort();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().sort(SortBuilders.fieldSort("testSortField1").order(SortOrder.ASC))
            .sort(SortBuilders.fieldSort("testSortField2").order(SortOrder.DESC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndGroupAndSortTest() {
        Query query = buildQueryAggregateAndGroupAndSort();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder4 = AggregationBuilders.stats("_statsFor_testField2").field("testField2");
        StatsAggregationBuilder aggBuilder3 = AggregationBuilders.stats("_statsFor_testField1").field("testField1");
        TermsAggregationBuilder aggBuilder2 = AggregationBuilders.terms("testField2").field("testField2").size(10000)
            .order(BucketOrder.key(false)).subAggregation(aggBuilder3).subAggregation(aggBuilder4);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField1").field("testField1").size(10000)
            .order(BucketOrder.key(true)).subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateAndSortTest() {
        Query query = buildQueryAggregateAndSort();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder1 = AggregationBuilders.stats("_statsFor_testField1").field("testField1");
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testField2").field("testField2");
        SearchSourceBuilder source = createSourceBuilder(0, 1).aggregation(aggBuilder1).aggregation(aggBuilder2)
            .sort(SortBuilders.fieldSort("testField1").order(SortOrder.ASC))
            .sort(SortBuilders.fieldSort("testField3").order(SortOrder.DESC));
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFieldAndSortTest() {
        Query query = buildQueryGroupByFieldAndSort();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        TermsAggregationBuilder aggBuilder2 = AggregationBuilders.terms("testField2").field("testField2").size(10000);
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField1").field("testField1").size(10000)
            .order(BucketOrder.key(true)).subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFunctionAndSortTest() {
        Query query = buildQueryGroupByFunctionAndSort();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        DateHistogramAggregationBuilder aggBuilder2 = AggregationBuilders.dateHistogram("testGroupLabel2").field("testGroupField2")
            .dateHistogramInterval(DateHistogramInterval.MONTH).format("M").order(BucketOrder.key(false));
        DateHistogramAggregationBuilder aggBuilder1 = AggregationBuilders.dateHistogram("testGroupLabel1").field("testGroupField1")
            .dateHistogramInterval(DateHistogramInterval.YEAR).format("yyyy").order(BucketOrder.key(true))
            .subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder().aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitTest() {
        Query query = buildQueryLimit();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder(0, 12);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryOffsetTest() {
        Query query = buildQueryOffset();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder(34, 10000);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitAndOffsetTest() {
        Query query = buildQueryLimitAndOffset();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder(34, 12);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAdvancedTest() {
        Query query = buildQueryAdvanced();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "testFilterValue"));
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testGroupField").field("testGroupField").size(12)
            .order(Arrays.asList(BucketOrder.count(false), BucketOrder.key(true))).subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(34, 1).fetchSource(new String[]{ "testField1", "testField2" }, null)
            .query(queryBuilder).aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryDuplicateFieldsTest() {
        Query query = buildQueryDuplicateFields();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        StatsAggregationBuilder aggBuilder2 = AggregationBuilders.stats("_statsFor_testField1").field("testField1");
        TermsAggregationBuilder aggBuilder1 = AggregationBuilders.terms("testField2").field("testField2").size(10000)
            .order(Arrays.asList(BucketOrder.count(false), BucketOrder.key(true))).subAggregation(aggBuilder2);
        SearchSourceBuilder source = createSourceBuilder(0, 1).fetchSource(new String[]{ "testField1", "testField2", "testField3" }, null)
            .aggregation(aggBuilder1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryFilterDateRangeTest() {
        Query query = buildQueryFilterDateRange();

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
        Query query = buildQueryFilterNumberRange();

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
        Query query = buildQueryFilterNestedAnd();

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
        Query query = buildQueryFilterNestedOr();

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
        Query query = buildQueryAggregateCountFieldAndFilter();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("testFilterField", "testFilterValue"))
            .must(QueryBuilders.existsQuery("testAggField"));
        SearchSourceBuilder source = createSourceBuilder(0, 1).query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryAggregateCountFieldAndMetricsTest() {
        Query query = buildQueryAggregateCountFieldAndMetrics();

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery("testAggField"));
        StatsAggregationBuilder aggBuilder = AggregationBuilders.stats("_statsFor_testAggField").field("testAggField");
        SearchSourceBuilder source = createSourceBuilder(0, 1).query(queryBuilder).aggregation(aggBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitZeroTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(0));

        // Elasticsearch-specific test:  do not set the query limit to zero!
        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder(0, 1);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitMaximumTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setLimitClause(new LimitClause(1000000));

        // Elasticsearch-specific test:  do not set the query limit to more than 10,000!
        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        expected = expected.scroll(TimeValue.timeValueMinutes(1));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryWithIdNotEqualsEmptyStringTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "_id"), "!=", ""));

        // Elasticsearch-specific test:  ignore _id not equals empty string
        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryWithCompoundIdNotEqualsEmptyStringTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "!=", "testFilterValue"),
            SingularWhereClause.fromNull(new FieldClause("testDatabase", "testTable", "_id"), "!="),
            SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "_id"), "!=", "")
        )));

        // Elasticsearch-specific test:  _id not equals empty string
        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("testFilterField", "testFilterValue")))
            .must(QueryBuilders.existsQuery("_id")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabase", "testTable", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryIgnoresFieldsFromOtherDatabasesAndTablesTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabaseA", "testTableX", Arrays.asList(
            new FieldClause("testDatabaseA", "testTableX", "testField1"),
            new FieldClause("testDatabaseA", "testTableY", "testField2"),
            new FieldClause("testDatabaseB", "testTableX", "testField3"),
            new FieldClause("testDatabaseB", "testTableY", "testField4")
        )));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder().fetchSource(new String[]{ "testField1" }, null);
        SearchRequest expected = createRequest("testDatabaseA", "testTableX", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryIgnoresSingularWhereFromOtherTableTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabaseA", "testTableX"));
        query.setWhereClause(SingularWhereClause.fromString(
            new FieldClause("testDatabaseA", "testTableY", "testField1"), "=", "a"));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabaseA", "testTableX", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryIgnoresSingularWhereFromOtherDatabaseTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabaseA", "testTableX"));
        query.setWhereClause(SingularWhereClause.fromString(
            new FieldClause("testDatabaseB", "testTableX", "testField1"), "=", "a"));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        SearchSourceBuilder source = createSourceBuilder();
        SearchRequest expected = createRequest("testDatabaseA", "testTableX", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryIgnoresMultipleWheresFromOtherDatabasesAndTablesTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabaseA", "testTableX"));
        query.setWhereClause(new AndWhereClause(Arrays.asList(
            SingularWhereClause.fromString(new FieldClause("testDatabaseA", "testTableX", "testField1"), "=", "a"),
            SingularWhereClause.fromString(new FieldClause("testDatabaseA", "testTableY", "testField2"), "=", "b"),
            SingularWhereClause.fromString(new FieldClause("testDatabaseB", "testTableX", "testField3"), "=", "c"),
            SingularWhereClause.fromString(new FieldClause("testDatabaseB", "testTableY", "testField4"), "=", "d")
        )));

        SearchRequest actual = ElasticsearchQueryConverter.convertQuery(query);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("testField1", "a")));
        SearchSourceBuilder source = createSourceBuilder().query(queryBuilder);
        SearchRequest expected = createRequest("testDatabaseA", "testTableX", source);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertMutationByIdQueryTest() {
        MutateQuery mutateQuery = buildMutationByIdQuery();
        UpdateRequest actual = ElasticsearchQueryConverter.convertMutationByIdQuery(mutateQuery);
        UpdateRequest expected = new UpdateRequest("testDatabase", "testTable", "testId")
            .doc(mutateQuery.getFieldsWithValues());
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertArrayAndObjectMutationByIdQueryTest() {
        MutateQuery mutateQuery = buildArrayAndObjectMutationByIdQuery();
        UpdateRequest actual = ElasticsearchQueryConverter.convertMutationByIdQuery(mutateQuery);
        UpdateRequest expected = new UpdateRequest("testDatabase", "testTable", "testId")
            .doc(mutateQuery.getFieldsWithValues());
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertInsertQueryTest() {
        MutateQuery mutateQuery = buildMutationByIdQuery();
        IndexRequest actual = ElasticsearchQueryConverter.convertMutationInsertQuery(mutateQuery);
        IndexRequest expected = new IndexRequest("testDatabase", "testTable", "testId")
                .source(mutateQuery.getFieldsWithValues());
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertArrayAndObjectInsertQueryTest() {
        MutateQuery mutateQuery = buildArrayAndObjectMutationByIdQuery();
        IndexRequest actual = ElasticsearchQueryConverter.convertMutationInsertQuery(mutateQuery);
        IndexRequest expected = new IndexRequest("testDatabase", "testTable", "testId")
                .source(mutateQuery.getFieldsWithValues());
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertDeleteByIdQueryRequestTest() {
        MutateQuery mutateQuery = buildMutationByIdQuery();
        DeleteRequest actual = ElasticsearchQueryConverter.convertMutationDeleteByIdQuery(mutateQuery);
        DeleteRequest expected = new DeleteRequest("testDatabase", "testTable", "testId");
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }

    @Test
    public void convertDeleteByFilterQueryRequestTest() {
        MutateQuery mutateQuery = buildMutationByFilterQuery();
        DeleteByQueryRequest actual = ElasticsearchQueryConverter.convertMutationDeleteByFilterQuery(mutateQuery);
        SelectClause selectClause = new SelectClause("testDatabase", "testTable");
        org.elasticsearch.index.query.QueryBuilder queryBuilder = ElasticsearchQueryConverter
                .convertWhereClauses(selectClause, List.of(SingularWhereClause.fromString(
                        new FieldClause("testDatabase", "testTable", "testFilterField1"),
                        "=", "testFilterValue1")));
        DeleteByQueryRequest expected = new DeleteByQueryRequest();
        expected.setQuery(queryBuilder);
        expected.getSearchRequest().indices("testDatabase").types("testTable");
        // This test fails without the toString (I don't know why)
        assertThat(actual.toString()).isEqualTo(expected.toString());
    }
}
