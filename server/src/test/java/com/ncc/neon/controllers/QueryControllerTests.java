package com.ncc.neon.controllers;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ncc.neon.NeonServerApplication;
import com.ncc.neon.models.queries.*;
import com.ncc.neon.models.results.TabularQueryResult;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Mono;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = NeonServerApplication.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class QueryControllerTests {

    @Autowired
    private WebTestClient webClient;

    @Test
    public void getDatabaseNamesTest() {
        this.webClient.get()
                .uri("/queryservice/databasenames/localhost/dummy")
                .accept(MediaType.APPLICATION_JSON_UTF8)
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .expectBody(List.class)
                .consumeWith(result -> {
                    Assertions.assertThat(result.getResponseBody()).isEqualTo(Arrays.asList(
                        new String[] { "A", "B", "C", "D" }));
                });
    }

    @Test
    public void getTableNamesTest() {
        this.webClient.get()
                .uri("/queryservice/tablenames/localhost/dummy/testDatabase")
                .accept(MediaType.APPLICATION_JSON_UTF8)
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .expectBody(List.class)
                .consumeWith(result -> {
                    Assertions.assertThat(result.getResponseBody()).isEqualTo(Arrays.asList(
                        new String[] { "X", "Y" }));
                });
    }

    @Test
    public void getFieldNamesAndTypesTest() {
        this.webClient.get()
                .uri("/queryservice/fields/types/localhost/dummy/testDatabase/testTable")
                .accept(MediaType.APPLICATION_JSON_UTF8)
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .expectBody(Map.class)
                .consumeWith(result -> {
                    Assertions.assertThat(result.getResponseBody()).isEqualTo(Map.ofEntries(
                        Map.entry("id", "id"),
                        Map.entry("blob", "object"),
                        Map.entry("date", "date"),
                        Map.entry("flag", "boolean"),
                        Map.entry("name", "keyword"),
                        Map.entry("size", "decimal"),
                        Map.entry("text", "text"),
                        Map.entry("type", "keyword")
                    ));
                });
    }

    @Test
    public void executeQueryTest() {
        SelectClause selectClause = new SelectClause("testDatabase", "testTable");
        WhereClause whereClause = SingularWhereClause.fromNull(
            new FieldClause("testDatabase", "testTable", "testWhereField"), "!=");
        ClusterClause clusterClause = new ClusterClause(0, "testType",
                List.of(List.of(Integer.valueOf(0), Integer.valueOf(25)), List.of(Integer.valueOf(26), Integer.valueOf(50))),
                "testAggregationName", "testFieldType", List.of("testFieldName1", "testFieldName2"));
        List<AggregateClause> aggregateClauses = List.of(new AggregateByFieldClause(
            new FieldClause("testDatabase", "testTable", "testAggregateField"), "testAggregateLabel", "count"));
        List<GroupByClause> groupByClauses = List.of(new GroupByFieldClause(
            new FieldClause("testDatabase", "testTable", "testGroupField")));
        List<OrderByClause> orderByClauses = List.of(new OrderByFieldClause(
            new FieldClause("testDatabase", "testTable", "testOrderField"), Order.DESCENDING));
        LimitClause limitClause = new LimitClause(12);
        OffsetClause offsetClause = new OffsetClause(34);
        boolean isDistinct = false;

        Query query = new Query(selectClause, whereClause, clusterClause, aggregateClauses, groupByClauses, orderByClauses,
            limitClause, offsetClause, List.of(), isDistinct);

        this.webClient.post()
                .uri("/queryservice/query/localhost/dummy")
                .accept(MediaType.APPLICATION_JSON_UTF8)
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(Mono.just(query), Query.class)
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .expectBody(TabularQueryResult.class)
                .consumeWith(result -> {
                    Assertions.assertThat(result.getResponseBody().getData()).isEqualTo(Arrays.asList(
                        Map.ofEntries(
                            Map.entry("fieldA", "value1"),
                            Map.entry("fieldB", 1)
                        ),
                        Map.ofEntries(
                            Map.entry("fieldA", "value2"),
                            Map.entry("fieldB", 2)
                        )
                    ));
                });
    }
}
