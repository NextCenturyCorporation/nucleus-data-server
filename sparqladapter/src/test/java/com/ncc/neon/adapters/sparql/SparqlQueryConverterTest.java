package com.ncc.neon.adapters.sparql;

import com.ncc.neon.adapters.QueryBuilder;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=SparqlQueryConverter.class)
public class SparqlQueryConverterTest extends QueryBuilder {

}
