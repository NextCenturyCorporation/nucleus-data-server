package com.ncc.neon.models.queries;

import com.ncc.neon.NeonServerApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;

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
        return new MutateQuery("testHost", "testType", "testDatabase", "testTable", "testIdField", "testId",
            new LinkedHashMap<String, Object>() {{
                put("testString", "a");
                put("testZero", 0);
                put("testInteger", 1);
                put("testDecimal", 0.5);
                put("testNegativeInteger", -1);
                put("testNegativeDecimal", -0.5);
                put("testTrue", true);
                put("testFalse", false);
                put("testEmptyArray", new ArrayList<Object>());
                put("testArray", new ArrayList<Object>() {{
                    add("b");
                    add(2);
                    add(true);
                    add(new LinkedHashMap<String, Object>() {{
                        put("testArrayObjectString", "c");
                        put("testArrayObjectInteger", 3);
                    }});
                }});
                put("testEmptyObject", new LinkedHashMap<String, Object>());
                put("testObject", new LinkedHashMap<String, Object>() {{
                    put("testObjectString", "d");
                    put("testObjectInteger", 4);
                    put("testObjectBoolean", true);
                    put("testObjectArray", new ArrayList<Object>() {{
                        add("e");
                        add(5);
                    }});
                }});
            }},
        null);
    }

}
