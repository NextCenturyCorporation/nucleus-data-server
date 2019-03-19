package com.ncc.neon.server.models.queries;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.ncc.neon.server.models.queries.AggregateClause;
import com.ncc.neon.server.models.queries.Filter;
import com.ncc.neon.server.models.queries.GroupByClause;
import com.ncc.neon.server.models.queries.GroupByFieldClause;
import com.ncc.neon.server.models.queries.LimitClause;
import com.ncc.neon.server.models.queries.OffsetClause;
import com.ncc.neon.server.models.queries.SingularWhereClause;
import com.ncc.neon.server.models.queries.SortClause;
import com.ncc.neon.server.models.queries.SortClauseOrder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@JsonTest
public class QueryJsonTest {

    @Autowired
    private JacksonTester<Query> json;

    @Test
    public void testSerializeQuery() throws Exception {
        assertThat(this.json.write(getQuery())).isEqualToJson("/json/serializedQuery.json");
    }

    @Test
    public void testDeserializeQuery() throws Exception {
        assertThat(this.json.read("/json/queryPost.json")).isEqualTo(getQuery());
    }

    private Query getQuery() {
        Filter filter = new Filter("ldc_uyg_jul_18", "ui_out", null, SingularWhereClause.fromNull("topic", "!="));
        List<String> fields = List.of("*");
        boolean aggregateArraysByElement = false;
        List<GroupByClause> groupByClauses = List.of(new GroupByFieldClause("topic", "topic"),
                new GroupByFieldClause("topic", "topic"));
        boolean isDistinct = false;
        List<AggregateClause> aggregates = List.of(new AggregateClause("_aggregation", "count", "*"));
        List<SortClause> sortClauses = List.of(new SortClause("_aggregation", SortClauseOrder.DESCENDING));
        LimitClause limitClause = new LimitClause(11);
        OffsetClause offsetClause = new OffsetClause(10);

        Query query = new Query(filter, aggregateArraysByElement, isDistinct, fields, aggregates, groupByClauses,
                sortClauses, limitClause, offsetClause);
        return query;
    }

}
