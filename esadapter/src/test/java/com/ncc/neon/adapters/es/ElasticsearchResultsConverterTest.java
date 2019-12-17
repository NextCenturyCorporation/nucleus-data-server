package com.ncc.neon.adapters.es;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ncc.neon.models.queries.AggregateByFieldClause;
import com.ncc.neon.models.queries.AggregateByGroupCountClause;
import com.ncc.neon.models.queries.AggregateByTotalCountClause;
import com.ncc.neon.models.queries.FieldClause;
import com.ncc.neon.models.queries.GroupByFieldClause;
import com.ncc.neon.models.queries.GroupByOperationClause;
import com.ncc.neon.models.queries.LimitClause;
import com.ncc.neon.models.queries.OffsetClause;
import com.ncc.neon.models.queries.Query;
import com.ncc.neon.models.queries.SelectClause;
import com.ncc.neon.models.queries.SingularWhereClause;
import com.ncc.neon.models.queries.OrderByFieldClause;
import com.ncc.neon.models.queries.OrderByOperationClause;
import com.ncc.neon.models.queries.Order;
import com.ncc.neon.models.results.TabularQueryResult;

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
@SpringBootTest(classes=ElasticsearchResultsConverter.class)
public class ElasticsearchResultsConverterTest {

    @Test
    public void convertResultsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsWithFilterDoesNotAffectOutputTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setWhereClause(SingularWhereClause.fromString(new FieldClause("testDatabase", "testTable", "testFilterField"), "=", "testFilterValue"));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsTotalCountAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByTotalCountClause("testCount")));

        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries());
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testCount", (long) 90)
            )
        ));
    }

    @Test
    public void convertResultsCountFieldAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testCount", "count")
        ));

        Aggregations aggregations = mock(Aggregations.class);
        when(aggregations.asMap()).thenReturn(Map.ofEntries());
        SearchHit[] hitArray = {};
        SearchHits hits = mock(SearchHits.class);
        when(hits.getHits()).thenReturn(hitArray);
        when(hits.getTotalHits()).thenReturn((long) 90);
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(aggregations);
        when(response.getHits()).thenReturn(hits);

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testCount", (long) 90)
            )
        ));
    }

    @Test
    public void convertResultsAvgAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAvg", "avg")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testAvg", 12.0)
            )
        ));
    }

    @Test
    public void convertResultsMaxAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testMax", "max")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testMax", 12.0)
            )
        ));
    }

    @Test
    public void convertResultsMinAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testMin", "min")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testMin", 12.0)
            )
        ));
    }

    @Test
    public void convertResultsSumAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testSum", "sum")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    public void convertResultsMultipleAggregationsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByTotalCountClause("testAggLabel1"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel2", "avg"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel3", "max"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel4", "min"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel5", "sum")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testAggLabel1", (long) 90),
                Map.entry("testAggLabel2", 12.0),
                Map.entry("testAggLabel3", 34.0),
                Map.entry("testAggLabel4", 56.0),
                Map.entry("testAggLabel5", 78.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void convertResultsGroupByTotalCountAggregationWithSingleBucketTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByTotalCountClause("testTotal")));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testTotal", (long) 90)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void convertResultsGroupByCountFieldAggregationWithSingleBucketTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testCount", (long) 21)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void convertResultsGroupByCountFieldAggregationWithMultipleBucketsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithBooleanGroupsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithNumberGroupsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithLimitTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithLimitAndOffsetTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithLimitAndSortByAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testCount", Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithLimitAndSortByFieldTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithLimitAndOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testCount", Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithLimitAndOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithOffsetTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setOffsetClause(new OffsetClause(1));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setOffsetClause(new OffsetClause(1));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testCount", Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setOffsetClause(new OffsetClause(1));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithSortByAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testCount", Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByCountFieldAggregationWithSortByFieldTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByNonCountAggregationWithSingleBucketTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testSum", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testGroupField", "testGroup1"),
                Map.entry("testSum", 12.0)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void convertResultsGroupByNonCountAggregationWithMultipleBucketsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testSum", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByNonCountAggregationWithLimitAndOffsetAndSortByAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testSum", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setOrderByClauses(Arrays.asList(new OrderByOperationClause("testSum", Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsGroupByNonCountAggregationWithLimitAndOffsetAndSortByFieldTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testSum", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))));
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), Order.ASCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsMultipleGroupsAndCountFieldAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testInnerGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testOuterGroupField")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testInnerGroupField"))
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsMultipleGroupsAndMultipleCountFieldAggregationsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testOuterGroupField"), "testOuterCount", "count"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testInnerGroupField"), "testInnerCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testOuterGroupField")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testInnerGroupField"))
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsMultipleGroupsAndNonCountAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testSum", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testOuterGroupField")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testInnerGroupField"))
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsMultipleGroupsAndMultipleAggregationsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testInnerGroupField"), "testAggLabel1", "count"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel2", "avg"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel3", "max"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel4", "min"),
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testAggLabel5", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testOuterGroupField")),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testInnerGroupField"))
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
        assertThat(results.getData()).isEqualTo(Arrays.asList(
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testAggLabel1", (long) 87),
                Map.entry("testAggLabel2", 78.12),
                Map.entry("testAggLabel3", 78.34),
                Map.entry("testAggLabel4", 78.56),
                Map.entry("testAggLabel5", 78.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup2"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testAggLabel1", (long) 65),
                Map.entry("testAggLabel2", 56.12),
                Map.entry("testAggLabel3", 56.34),
                Map.entry("testAggLabel4", 56.56),
                Map.entry("testAggLabel5", 56.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupB"),
                Map.entry("testAggLabel1", (long) 43),
                Map.entry("testAggLabel2", 34.12),
                Map.entry("testAggLabel3", 34.34),
                Map.entry("testAggLabel4", 34.56),
                Map.entry("testAggLabel5", 34.78)
            ),
            Map.ofEntries(
                Map.entry("testOuterGroupField", "testOuterGroup1"),
                Map.entry("testInnerGroupField", "testInnerGroupA"),
                Map.entry("testAggLabel1", (long) 21),
                Map.entry("testAggLabel2", 12.12),
                Map.entry("testAggLabel3", 12.34),
                Map.entry("testAggLabel4", 12.56),
                Map.entry("testAggLabel5", 12.78)
            )
        ));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void convertResultsDateGroupsAndCountFieldAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByGroupCountClause("testYear", "testCount")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testYear", "year")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDateGroupsAndNonCountAggregationTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testAggField"), "testSum", "sum")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testDateField"), "testYear", "year")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsMultipleDateGroupsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByGroupCountClause("testYear", "testCount")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testDateField"), "testMonth", "month"),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testDateField"), "testYear", "year")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDateAndNonDateGroupsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(
            new AggregateByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"), "testCount", "count")
        ));
        query.setGroupByClauses(Arrays.asList(
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testDateField"), "testYear", "year"),
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField"))
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsNonDateAndDateGroupsTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable"));
        query.setAggregateClauses(Arrays.asList(new AggregateByGroupCountClause("testYear", "testCount")));
        query.setGroupByClauses(Arrays.asList(
            new GroupByFieldClause(new FieldClause("testDatabase", "testTable", "testGroupField")),
            new GroupByOperationClause(new FieldClause("testDatabase", "testTable", "testDateField"), "testYear", "year")
        ));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDistinctValuesTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testFieldA"),
            new FieldClause("testDatabase", "testTable", "testFieldB")
        )));
        query.setDistinct(true);

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDistinctValuesWithLimitTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testFieldA"),
            new FieldClause("testDatabase", "testTable", "testFieldB")
        )));
        query.setDistinct(true);
        query.setLimitClause(new LimitClause(2));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDistinctValuesWithOffsetTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testFieldA"),
            new FieldClause("testDatabase", "testTable", "testFieldB")
        )));
        query.setDistinct(true);
        query.setOffsetClause(new OffsetClause(1));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDistinctValuesWithSortTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testFieldA"),
            new FieldClause("testDatabase", "testTable", "testFieldB")
        )));
        query.setDistinct(true);
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testFieldA"), Order.DESCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDistinctValuesWithLimitAndOffsetAndSortTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testFieldA"),
            new FieldClause("testDatabase", "testTable", "testFieldB")
        )));
        query.setDistinct(true);
        query.setLimitClause(new LimitClause(2));
        query.setOffsetClause(new OffsetClause(1));
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testFieldA"), Order.DESCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
    public void convertResultsDistinctValuesWithSortAndNumberDataTest() {
        Query query = new Query();
        query.setSelectClause(new SelectClause("testDatabase", "testTable", Arrays.asList(
            new FieldClause("testDatabase", "testTable", "testFieldA"),
            new FieldClause("testDatabase", "testTable", "testFieldB")
        )));
        query.setDistinct(true);
        query.setOrderByClauses(Arrays.asList(new OrderByFieldClause(new FieldClause("testDatabase", "testTable", "testFieldA"), Order.DESCENDING)));

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

        TabularQueryResult results = ElasticsearchResultsConverter.convertResults(query, response);
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
