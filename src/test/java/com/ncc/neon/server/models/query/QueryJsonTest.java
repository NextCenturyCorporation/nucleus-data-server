package com.ncc.neon.server.models.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ncc.neon.server.models.query.Query;
import com.ncc.neon.server.models.query.clauses.AggregateClause;
import com.ncc.neon.server.models.query.clauses.GroupByClause;
import com.ncc.neon.server.models.query.clauses.GroupByFieldClause;
import com.ncc.neon.server.models.query.clauses.LimitClause;
import com.ncc.neon.server.models.query.clauses.OffsetClause;
import com.ncc.neon.server.models.query.clauses.SortClause;
import com.ncc.neon.server.models.query.clauses.SortOrder;
import com.ncc.neon.server.models.query.filter.Filter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.json.ObjectContent;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * QueryTests
 */
@RunWith(SpringRunner.class)
@JsonTest
public class QueryJsonTest {

	@Autowired
	private JacksonTester<Query> json;

	@Test
	public void testSerialize() throws Exception {
		/*
		 * Query query = new Query(filter, aggregateArraysByElement, isDistinct, fields,
		 * aggregates, groupByClauses, sortClauses, limitClause, offsetClause);
		 * 
		 * // Assert against a `.json` file in the same package as the test
		 * assertThat(this.json.write(query)).isEqualToJson("expected.json"); // Or use
		 * JSON path based assertions
		 * assertThat(this.json.write(query)).hasJsonPathStringValue("@.make");
		 * assertThat(this.json.write(query)).extractingJsonPathStringValue("@.make")
		 * .isEqualTo("Honda");
		 */
	}

	@Test
	public void testDeserialize() throws Exception {

		Filter filter = null;
		List<String> fields = List.of("*");
		boolean aggregateArraysByElement = false;
		List<GroupByClause> groupByClauses = List.of(new GroupByFieldClause("topic","topic"),new GroupByFieldClause("topic","topic"));
		boolean isDistinct = false;
		// TODO: test emptry contructor list of
		List<AggregateClause> aggregates = List.of(new AggregateClause("_aggregation", "count", "*"));
		List<SortClause> sortClauses = List.of(new SortClause("_aggregation", SortOrder.DESCENDING));
		LimitClause limitClause = new LimitClause(11);
		OffsetClause offsetClause = new OffsetClause(10);
		
		Query query = new Query(filter, aggregateArraysByElement, isDistinct, fields, aggregates, groupByClauses,
				sortClauses, limitClause, offsetClause);

		assertThat(this.json.read("/json/queryPost.json")).isEqualTo(query);
		// assertThat(this.json.parseObject(content).getMake()).isEqualTo("Ford");
	}

}