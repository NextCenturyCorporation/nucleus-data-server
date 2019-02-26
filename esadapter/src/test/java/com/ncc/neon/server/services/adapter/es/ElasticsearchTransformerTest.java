package com.ncc.neon.server.services.adapter.es;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.QueryOptions;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.GroupByFunctionClause;
import com.ncc.neon.server.models.query.clauses.LimitClause;
import com.ncc.neon.server.models.query.clauses.OffsetClause;
import com.ncc.neon.server.models.query.clauses.SingularWhereClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.clauses.SortOrder;
import com.ncc.neon.server.models.query.filter.Filter;
import com.ncc.neon.server.models.query.result.TabularQueryResult;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=ElasticsearchTransformer.class)
public class ElasticsearchTransformerTest {

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
    public void transformResultsCountAllAggregationTest() {
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
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "testAggregateField")));
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
        query.setAggregates(Arrays.asList(new AggregateClause("testAvg", "avg", "testAggregateField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getAvg()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats)
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
        query.setAggregates(Arrays.asList(new AggregateClause("testMax", "max", "testAggregateField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getMax()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats)
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
        query.setAggregates(Arrays.asList(new AggregateClause("testMin", "min", "testAggregateField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getMin()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats)
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
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggregateField")));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getSum()).thenReturn(12.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats)
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
            new AggregateClause("testAggregateName1", "count", "*"),
            new AggregateClause("testAggregateName2", "avg", "testAggregateField"),
            new AggregateClause("testAggregateName3", "max", "testAggregateField"),
            new AggregateClause("testAggregateName4", "min", "testAggregateField"),
            new AggregateClause("testAggregateName5", "sum", "testAggregateField")
        ));
        QueryOptions options = new QueryOptions();

        Stats stats = mock(Stats.class);
        when(stats.getAvg()).thenReturn(12.0);
        when(stats.getMax()).thenReturn(34.0);
        when(stats.getMin()).thenReturn(56.0);
        when(stats.getSum()).thenReturn(78.0);
        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats)
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
                Map.entry("testAggregateName1", (long) 90),
                Map.entry("testAggregateName2", 12.0),
                Map.entry("testAggregateName3", 34.0),
                Map.entry("testAggregateName4", 56.0),
                Map.entry("testAggregateName5", 78.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsGroupByCountAggregationWithSingleBucketTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsGroupByCountAggregationWithMultipleBucketsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsGroupByCountAggregationWithBooleanGroupsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsGroupByCountAggregationWithNumberGroupsTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsGroupByCountAggregationWithLimitTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsGroupByCountAggregationWithLimitAndOffsetTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsGroupByCountAggregationWithLimitAndSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", SortOrder.ASCENDING)));
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
    public void transformResultsGroupByCountAggregationWithLimitAndSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", SortOrder.ASCENDING)));
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
    public void transformResultsGroupByCountAggregationWithLimitAndOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", SortOrder.ASCENDING)));
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
    public void transformResultsGroupByCountAggregationWithLimitAndOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", SortOrder.ASCENDING)));
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
    public void transformResultsGroupByCountAggregationWithOffsetTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsGroupByCountAggregationWithOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", SortOrder.ASCENDING)));
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
    public void transformResultsGroupByCountAggregationWithOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", SortOrder.ASCENDING)));
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
    public void transformResultsGroupByCountAggregationWithSortByAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setSortClauses(Arrays.asList(new SortClause("testCount", SortOrder.ASCENDING)));
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
    public void transformResultsGroupByCountAggregationWithSortByFieldTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", SortOrder.ASCENDING)));
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
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggregateField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats1)
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
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggregateField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("testGroup1");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats2)
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
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggregateField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testSum", SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats2)
        ));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Stats stats3 = mock(Stats.class);
        when(stats3.getSum()).thenReturn(56.0);
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats3)
        ));
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Stats stats4 = mock(Stats.class);
        when(stats4.getSum()).thenReturn(78.0);
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats4)
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
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggregateField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause("testGroupField", "Test Group Field")));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setSortClauses(Arrays.asList(new SortClause("testGroupField", SortOrder.ASCENDING)));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKey()).thenReturn("c");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats2)
        ));
        Terms.Bucket bucket2 = mock(Terms.Bucket.class);
        when(bucket2.getAggregations()).thenReturn(bucketAggregations2);
        when(bucket2.getDocCount()).thenReturn((long) 43);
        when(bucket2.getKey()).thenReturn("d");
        Stats stats3 = mock(Stats.class);
        when(stats3.getSum()).thenReturn(56.0);
        Aggregations bucketAggregations3 = mock(Aggregations.class);
        when(bucketAggregations3.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats3)
        ));
        Terms.Bucket bucket3 = mock(Terms.Bucket.class);
        when(bucket3.getAggregations()).thenReturn(bucketAggregations3);
        when(bucket3.getDocCount()).thenReturn((long) 65);
        when(bucket3.getKey()).thenReturn("a");
        Stats stats4 = mock(Stats.class);
        when(stats4.getSum()).thenReturn(78.0);
        Aggregations bucketAggregations4 = mock(Aggregations.class);
        when(bucketAggregations4.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats4)
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
    public void transformResultsMultipleGroupsAndCountAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
    public void transformResultsMultipleGroupsAndNonCountAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggregateField")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause("testOuterGroupField", "Test Outer Group Field"),
            new GroupByFieldClause("testInnerGroupField", "Test Inner Group Field")
        ));
        QueryOptions options = new QueryOptions();

        Stats stats1A = mock(Stats.class);
        when(stats1A.getSum()).thenReturn(12.0);
        Aggregations nestedBucketAggregations1A = mock(Aggregations.class);
        when(nestedBucketAggregations1A.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats1A)
        ));
        Terms.Bucket nestedBucket1A = mock(Terms.Bucket.class);
        when(nestedBucket1A.getAggregations()).thenReturn(nestedBucketAggregations1A);
        when(nestedBucket1A.getDocCount()).thenReturn((long) 21);
        when(nestedBucket1A.getKey()).thenReturn("testInnerGroupA");
        Stats stats1B = mock(Stats.class);
        when(stats1B.getSum()).thenReturn(34.0);
        Aggregations nestedBucketAggregations1B = mock(Aggregations.class);
        when(nestedBucketAggregations1B.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats1B)
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
            Map.entry("_statsFor_testAggregateField", stats2A)
        ));
        Terms.Bucket nestedBucket2A = mock(Terms.Bucket.class);
        when(nestedBucket2A.getAggregations()).thenReturn(nestedBucketAggregations2A);
        when(nestedBucket2A.getDocCount()).thenReturn((long) 65);
        when(nestedBucket2A.getKey()).thenReturn("testInnerGroupA");
        Stats stats2B = mock(Stats.class);
        when(stats2B.getSum()).thenReturn(78.0);
        Aggregations nestedBucketAggregations2B = mock(Aggregations.class);
        when(nestedBucketAggregations2B.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats2B)
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
            new AggregateClause("testAggregateName1", "count", "*"),
            new AggregateClause("testAggregateName2", "avg", "testAggregateField"),
            new AggregateClause("testAggregateName3", "max", "testAggregateField"),
            new AggregateClause("testAggregateName4", "min", "testAggregateField"),
            new AggregateClause("testAggregateName5", "sum", "testAggregateField")
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
            Map.entry("_statsFor_testAggregateField", stats1A)
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
            Map.entry("_statsFor_testAggregateField", stats1B)
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
            Map.entry("_statsFor_testAggregateField", stats2A)
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
            Map.entry("_statsFor_testAggregateField", stats2B)
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
                Map.entry("testAggregateName1", (long) 87),
                Map.entry("testAggregateName2", 78.12),
                Map.entry("testAggregateName3", 78.34),
                Map.entry("testAggregateName4", 78.56),
                Map.entry("testAggregateName5", 78.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testAggregateName1", (long) 65),
                Map.entry("testAggregateName2", 56.12),
                Map.entry("testAggregateName3", 56.34),
                Map.entry("testAggregateName4", 56.56),
                Map.entry("testAggregateName5", 56.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testAggregateName1", (long) 43),
                Map.entry("testAggregateName2", 34.12),
                Map.entry("testAggregateName3", 34.34),
                Map.entry("testAggregateName4", 34.56),
                Map.entry("testAggregateName5", 34.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testAggregateName1", (long) 21),
                Map.entry("testAggregateName2", 12.12),
                Map.entry("testAggregateName3", 12.34),
                Map.entry("testAggregateName4", 12.56),
                Map.entry("testAggregateName5", 12.78)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void transformResultsDateGroupsAndCountAggregationTest() {
        Query query = new Query();
        query.setFilter(new Filter("testDatabase", "testTable"));
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
        query.setAggregates(Arrays.asList(new AggregateClause("testSum", "sum", "testAggregateField")));
        query.setGroupByClauses(Arrays.asList(new GroupByFunctionClause("testYear", "year", "testDateField")));
        QueryOptions options = new QueryOptions();

        Stats stats1 = mock(Stats.class);
        when(stats1.getSum()).thenReturn(12.0);
        Aggregations bucketAggregations1 = mock(Aggregations.class);
        when(bucketAggregations1.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats1)
        ));
        Terms.Bucket bucket1 = mock(Terms.Bucket.class);
        when(bucket1.getAggregations()).thenReturn(bucketAggregations1);
        when(bucket1.getDocCount()).thenReturn((long) 21);
        when(bucket1.getKeyAsString()).thenReturn("2018");
        Stats stats2 = mock(Stats.class);
        when(stats2.getSum()).thenReturn(34.0);
        Aggregations bucketAggregations2 = mock(Aggregations.class);
        when(bucketAggregations2.asMap()).thenReturn(Map.ofEntries(
            Map.entry("_statsFor_testAggregateField", stats2)
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
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
        query.setAggregates(Arrays.asList(new AggregateClause("testCount", "count", "*")));
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
        query.setSortClauses(Arrays.asList(new SortClause("testFieldA", SortOrder.DESCENDING)));
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
        query.setSortClauses(Arrays.asList(new SortClause("testFieldA", SortOrder.DESCENDING)));
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
        query.setSortClauses(Arrays.asList(new SortClause("testFieldA", SortOrder.DESCENDING)));
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
