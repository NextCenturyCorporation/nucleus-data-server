package com.ncc.neon.models.queries;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.ncc.neon.NeonServerApplication;
import com.ncc.neon.models.queries.AggregateClause;
import com.ncc.neon.models.queries.GroupByClause;
import com.ncc.neon.models.queries.GroupByFieldClause;
import com.ncc.neon.models.queries.LimitClause;
import com.ncc.neon.models.queries.OffsetClause;
import com.ncc.neon.models.queries.SingularWhereClause;
import com.ncc.neon.models.queries.OrderByClause;
import com.ncc.neon.models.queries.Order;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = NeonServerApplication.class)
@JsonTest
public class QueryJsonTest {

    @Autowired
    private JacksonTester<Query> json;

    @Test
    public void testSerializeQuery() throws Exception {
        assertThat(this.json.write(getQuery())).isEqualToJson("/json/queryPost.json");
    }

    @Test
    public void testDeserializeQuery() throws Exception {
        assertThat(this.json.read("/json/queryPost.json")).isEqualTo(getQuery());
    }

    @Test
    public void testDeserializeNegativeDoubleQuery() throws Exception {
        SelectClause selectClause = new SelectClause("testDatabase", "testTable");
        WhereClause whereClause = SingularWhereClause.fromDouble(
            new FieldClause("testDatabase", "testTable", "testField"), "!=", -1234.5678);
        List<AggregateClause> aggregateClauses = List.of();
        List<GroupByClause> groupByClauses = List.of();
        List<OrderByClause> orderByClauses = List.of();
        LimitClause limitClause = new LimitClause(10);
        OffsetClause offsetClause = new OffsetClause(0);
        boolean isDistinct = false;

        Query query = new Query(selectClause, whereClause, aggregateClauses, groupByClauses, orderByClauses,
            limitClause, offsetClause, List.of(), isDistinct);

        assertThat(this.json.read("/json/queryWithNegativeDouble.json")).isEqualTo(query);
    }

    @Test
    public void testDeserializeNegativeIntQuery() throws Exception {
        SelectClause selectClause = new SelectClause("testDatabase", "testTable");
        WhereClause whereClause = SingularWhereClause.fromDouble(
            new FieldClause("testDatabase", "testTable", "testField"), "!=", -1234);
        List<AggregateClause> aggregateClauses = List.of();
        List<GroupByClause> groupByClauses = List.of();
        List<OrderByClause> orderByClauses = List.of();
        LimitClause limitClause = new LimitClause(10);
        OffsetClause offsetClause = new OffsetClause(0);
        boolean isDistinct = false;

        Query query = new Query(selectClause, whereClause, aggregateClauses, groupByClauses, orderByClauses,
            limitClause, offsetClause, List.of(), isDistinct);

        assertThat(this.json.read("/json/queryWithNegativeInt.json")).isEqualTo(query);
    }

    private Query getQuery() {
        SelectClause selectClause = new SelectClause("testDatabase", "testTable");
        WhereClause whereClause = SingularWhereClause.fromNull(
            new FieldClause("testDatabase", "testTable", "testWhereField"), "!=");
        List<AggregateClause> aggregateClauses = List.of(new AggregateByFieldClause(
            new FieldClause("testDatabase", "testTable", "testAggregateField"), "testAggregateLabel", "count"));
        List<GroupByClause> groupByClauses = List.of(new GroupByFieldClause(
            new FieldClause("testDatabase", "testTable", "testGroupField")));
        List<OrderByClause> orderByClauses = List.of(new OrderByFieldClause(
            new FieldClause("testDatabase", "testTable", "testOrderField"), Order.DESCENDING));
        LimitClause limitClause = new LimitClause(12);
        OffsetClause offsetClause = new OffsetClause(34);
        boolean isDistinct = false;

        Query query = new Query(selectClause, whereClause, aggregateClauses, groupByClauses, orderByClauses,
            limitClause, offsetClause, List.of(), isDistinct);

        return query;
    }

}
