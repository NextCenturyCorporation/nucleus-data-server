package com.ncc.neon.server.controllers;

import java.util.List;
import java.util.Map;

import com.ncc.neon.server.models.queries.AggregateClause;
import com.ncc.neon.server.models.queries.Filter;
import com.ncc.neon.server.models.queries.GroupByClause;
import com.ncc.neon.server.models.queries.GroupByFieldClause;
import com.ncc.neon.server.models.queries.LimitClause;
import com.ncc.neon.server.models.queries.OffsetClause;
import com.ncc.neon.server.models.queries.Query;
import com.ncc.neon.server.models.queries.SingularWhereClause;
import com.ncc.neon.server.models.queries.SortClause;
import com.ncc.neon.server.models.queries.SortOrder;
import com.ncc.neon.server.models.results.TabularQueryResult;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Mono;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureWebTestClient
public class QueryControllerTests {

    @Autowired
    private WebTestClient webClient;

    @Test
    public void getTablesAndFieldsTest() {
        this.webClient.get()
                .uri("/queryservice/tablesandfields/localhost/dummy/ldc_uyg_jul_18")
                .accept(MediaType.APPLICATION_JSON_UTF8)
                .exchange()
                .expectStatus()
                .isOk()
                .expectHeader()
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .expectBody(Map.class)
                .consumeWith(result -> {
                    // Good map assertions examples
                    // https://github.com/joel-costigliola/assertj-examples/blob/master/assertions-examples/src/test/java/org/assertj/examples/MapAssertionsExamples.java
                    @SuppressWarnings("unchecked")
                    Map<String, List<String>> map = result.getResponseBody();
                    Assertions.assertThat(map).isNotEmpty().hasSize(2);
                    Assertions.assertThat(map).containsKey("1");
                });
    }

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
                    // https://github.com/joel-costigliola/assertj-examples/blob/master/assertions-examples/src/test/java/org/assertj/examples/IterableAssertionsExamples.java
                    @SuppressWarnings("unchecked")
                    List<String> list = result.getResponseBody();
                    Assertions.assertThat(list).isNotEmpty().hasSize(6);
                    Assertions.assertThat(list).doesNotContain("D");
                });

    }

    @Test
    public void executeQueryTest() {
        Filter filter = new Filter("ldc_uyg_jul_18", "ui_out", null, SingularWhereClause.fromNull("topic", "!="));
        List<String> fields = List.of("*");
        boolean aggregateArraysByElement = false;
        List<GroupByClause> groupByClauses = List.of(new GroupByFieldClause("topic", "topic"),
                new GroupByFieldClause("topic", "topic"));
        boolean isDistinct = false;
        List<AggregateClause> aggregates = List.of(new AggregateClause("_aggregation", "count", "*"));
        List<SortClause> sortClauses = List.of(new SortClause("_aggregation", SortOrder.DESCENDING));
        LimitClause limitClause = new LimitClause(11);
        OffsetClause offsetClause = new OffsetClause(10);

        Query query = new Query(filter, aggregateArraysByElement, isDistinct, fields, aggregates, groupByClauses,
                sortClauses, limitClause, offsetClause);

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
                    TabularQueryResult table = result.getResponseBody();
                    List<Map<String, Object>> data = table.getData();
                    Assertions.assertThat(data).isNotEmpty().hasSize(2);
                });
    }
}
