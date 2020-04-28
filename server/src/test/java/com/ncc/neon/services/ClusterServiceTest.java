package com.ncc.neon.services;

import com.ncc.neon.NeonServerApplication;
import com.ncc.neon.models.queries.ClusterClause;
import com.ncc.neon.models.results.TabularQueryResult;
import org.json.JSONException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = NeonServerApplication.class)
@JsonTest
public class ClusterServiceTest {

    @Autowired
    private JacksonTester<ClusterClause> json;
    
    @Autowired
    private JacksonTester<List<Map<String, Object>>> inputJson;

    private static ClusterService clusterService;

    @BeforeClass
    public static void setup() {
        clusterService = new ClusterService();
    }

    @Test
    public void numberAggregationTest1() {
        try {
            ClusterClause clusterClause = this.json.read("/json/clusterClause1.json").getObject();
            clusterService.setClusterClause(clusterClause);
            TabularQueryResult input = new TabularQueryResult(this.inputJson
                    .read("/json/numberAggregationInput1.json").getObject());
            TabularQueryResult output = clusterService.cluster(input);
            String expectedOutputJson = this.inputJson.write(this.inputJson
                    .read("/json/numberAggregationOutput1.json").getObject()).getJson();
            String outputJson = this.inputJson.write(output.getData()).getJson();
            JSONAssert.assertEquals(expectedOutputJson, outputJson, true);
        } catch (IOException | JSONException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void numberAggregationTest2() {
        try {
            ClusterClause clusterClause = this.json.read("/json/clusterClause1.json").getObject();
            clusterService.setClusterClause(clusterClause);
            TabularQueryResult input = new TabularQueryResult(this.inputJson
                    .read("/json/numberAggregationInput2.json").getObject());
            TabularQueryResult output = clusterService.cluster(input);
            String expectedOutputJson = this.inputJson.write(this.inputJson
                    .read("/json/numberAggregationOutput2.json").getObject()).getJson();
            String outputJson = this.inputJson.write(output.getData()).getJson();
            JSONAssert.assertEquals(expectedOutputJson, outputJson, true);
        } catch (IOException | JSONException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void numberAggregationTest3() {
        try {
            ClusterClause clusterClause = this.json.read("/json/clusterClause2.json").getObject();
            clusterService.setClusterClause(clusterClause);
            TabularQueryResult input = new TabularQueryResult(this.inputJson
                    .read("/json/numberAggregationInput3.json").getObject());
            TabularQueryResult output = clusterService.cluster(input);
            String expectedOutputJson = this.inputJson.write(this.inputJson
                    .read("/json/numberAggregationOutput3.json").getObject()).getJson();
            String outputJson = this.inputJson.write(output.getData()).getJson();
            JSONAssert.assertEquals(expectedOutputJson, outputJson, true);
        } catch (IOException | JSONException e) {
            e.printStackTrace();
            fail();
        }
    }
}
