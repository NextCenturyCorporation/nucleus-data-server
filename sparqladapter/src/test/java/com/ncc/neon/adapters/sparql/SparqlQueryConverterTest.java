package com.ncc.neon.adapters.sparql;

import com.ncc.neon.adapters.QueryBuilder;
import com.ncc.neon.models.queries.Query;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=SparqlQueryConverter.class)
public class SparqlQueryConverterTest extends QueryBuilder {

    @Test
    public void convertQueryBaseTest() {
        Query query = buildQueryBase();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT * WHERE { ?s ?p ?o }";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryBaseDistinctTest() {
        Query query = buildQueryDistinctSparql();
        query.setDistinct(true);
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryWhereTest() {
        Query query = buildQueryFilterEqualsValueSparql();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT ?s ?p ?o WHERE { ?o ?testType ?testObject} ";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryMultipleWhereTest() {
        Query query = buildQueryMultipleFilterEqualsValueSparql();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT ?s ?p ?o WHERE { ?o ?testObjectType ?testObject \n" +
                " ?p ?testPredicateType ?testPredicate} ";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryGroupByFieldTest() {
        Query query = buildQueryGroupByFieldSparql();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT * WHERE { ?s ?p ?o } GROUP BY ?o";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortAscendingTest() {
        Query query = buildQuerySortAscendingSparql();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT * WHERE { ?s ?p ?o } ORDER BY ASC(?o)";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQuerySortDescendingTest() {
        Query query = buildQuerySortDescendingSparql();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT * WHERE { ?s ?p ?o } ORDER BY DESC(?o)";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryLimitTest() {
        Query query = buildQueryLimit();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT * WHERE { ?s ?p ?o } LIMIT 12";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void convertQueryOffsetTest() {
        Query query = buildQueryOffset();
        String actual = SparqlQueryConverter.convertQuery(query);
        String expected = "SELECT * WHERE { ?s ?p ?o } OFFSET 34";
        assertThat(actual).isEqualTo(expected);
    }
}
