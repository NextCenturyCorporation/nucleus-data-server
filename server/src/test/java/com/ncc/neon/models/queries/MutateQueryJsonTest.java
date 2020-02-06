package com.ncc.neon.models.queries;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.ncc.neon.NeonServerApplication;

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
public class MutateQueryJsonTest {

    @Autowired
    private JacksonTester<MutateQuery> json;

    @Test
    public void testSerializeMutationQuery() throws Exception {
        assertThat(this.json.write(getQuery())).isEqualToJson("/json/mutateQuery.json");
    }

    @Test
    public void testDeserializeMutationQuery() throws Exception {
        assertThat(this.json.read("/json/mutateQuery.json")).isEqualTo(getQuery());
    }

    private MutateQuery getQuery() {
        return new MutateQuery("testHost", "testType", "testDatabase", "testTable", "testId",
            new LinkedHashMap<String, Object>(){{
                put("testString", "string");
                put("testZero", 0);
                put("testInteger", 1234);
                put("testDecimal", 56.78);
                put("testNegativeInteger", -4321);
                put("testNegativeDecimal", -87.65);
                put("testTrue", true);
                put("testFalse", false);
                put("testEmptyArray", new ArrayList<Object>());
                put("testArray", new ArrayList<Object>(){{
                    add("a");
                    add("b");
                    add("c");
                    add("d");
                }});
                put("testEmptyObject", new LinkedHashMap<String, Object>());
                put("testObject", new LinkedHashMap<String, Object>(){{
                    put("testPropertyOne", "x");
                    put("testPropertyTwo", "y");
                }});
            }}
        );
    }

}
