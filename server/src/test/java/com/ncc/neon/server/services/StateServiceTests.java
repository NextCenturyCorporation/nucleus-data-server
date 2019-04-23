package com.ncc.neon.server.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = StateService.class)
public class StateServiceTests {

    private static Map DATA_NEON_STRUCTURE;
    private static Map DATA_NULL_STRUCTURE;
    private static Map DATA_TEST_STRUCTURE;

    private static String JSON_STATE_NAME_1 = "jsonState1";
    private static String JSON_STATE_NAME_2 = "jsonState2";
    private static String JSON_STATE_NAME_3 = "jsonState3";
    private static String JSON_STATE_NAME_4 = "jsonState4";
    private static String JSON_STATE_NAME_5 = "jsonState5";
    private static String JSON_STATE_NAME_6 = "jsonState6";
    private static String JSON_STATE_NAME_7 = "jsonState7";

    private static String YAML_STATE_NAME_1 = "yamlState1";
    private static String YAML_STATE_NAME_2 = "yamlState2";
    private static String YAML_STATE_NAME_3 = "yamlState3";
    private static String YAML_STATE_NAME_4 = "yamlState4";
    private static String YAML_STATE_NAME_5 = "yamlState5";
    private static String YAML_STATE_NAME_6 = "yamlState6";
    private static String YAML_STATE_NAME_7 = "yamlState7";

    private static ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private static String STATE_DIRECTORY = "src/test/resources/states";
    private static StateService STATE_SERVICE;

    private static Path EMPTY_STATE_DIRECTORY;
    private static StateService EMPTY_STATE_SERVICE;

    @BeforeClass
    public static void setup() {
        try {
            EMPTY_STATE_DIRECTORY = Files.createTempDirectory("no_states_");
            EMPTY_STATE_SERVICE = new StateService(EMPTY_STATE_DIRECTORY.toString());
        }
        catch (IOException e) {
            fail(e.toString());
        }

        STATE_SERVICE = new StateService(STATE_DIRECTORY);
        try {
            DATA_NULL_STRUCTURE = JSON_MAPPER.readValue("{}", LinkedHashMap.class);
            DATA_NEON_STRUCTURE = JSON_MAPPER.readValue("{ \"dashboards\": {}, \"datastores\": {}, \"layouts\": [], \"options\": {} }",
                LinkedHashMap.class);
            DATA_TEST_STRUCTURE = JSON_MAPPER.readValue(
                "{ \"list\": [\"a\", \"b\", \"c\"], \"number\": 123, \"object\": { \"key\": \"value\" }, \"string\": \"test\" }",
                LinkedHashMap.class);
        }
        catch (IOException e) {
            fail(e.toString());
        }
    }

    @Test
    public void deleteStateTest() {
        try {
            File testFile = new File(STATE_DIRECTORY + "/testStateName.yaml");
            testFile.createNewFile();
            assertThat(testFile.exists()).isEqualTo(true);

            boolean successful = STATE_SERVICE.deleteState("testStateName");
            assertThat(successful).isEqualTo(true);

            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException e) {
            fail(e.toString());
        }
    }

    @Test
    public void findStateNamesTest() {
        assertThat(STATE_SERVICE.findStateNames()).isEqualTo(new String[] { JSON_STATE_NAME_1, JSON_STATE_NAME_2, JSON_STATE_NAME_3,
            JSON_STATE_NAME_4, JSON_STATE_NAME_5, JSON_STATE_NAME_6, JSON_STATE_NAME_7, YAML_STATE_NAME_1, YAML_STATE_NAME_2,
            YAML_STATE_NAME_3, YAML_STATE_NAME_4, YAML_STATE_NAME_5, YAML_STATE_NAME_6, YAML_STATE_NAME_7 });
    }

    @Test
    public void findStateNamesWithNoPreviousStatesTest() {
        assertThat(EMPTY_STATE_SERVICE.findStateNames()).isEqualTo(new String[0]);
    }

    @Test
    public void loadStateWithJsonFormatTest() {
        assertThat(STATE_SERVICE.loadState(JSON_STATE_NAME_1)).isEqualTo(DATA_NEON_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(JSON_STATE_NAME_2)).isEqualTo(DATA_NULL_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(JSON_STATE_NAME_3)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(JSON_STATE_NAME_4)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(JSON_STATE_NAME_5)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(JSON_STATE_NAME_6)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(JSON_STATE_NAME_7)).isEqualTo(DATA_TEST_STRUCTURE);
    }

    @Test
    public void loadStateWithYamlFormatTest() {
        assertThat(STATE_SERVICE.loadState(YAML_STATE_NAME_1)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(YAML_STATE_NAME_2)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(YAML_STATE_NAME_3)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(YAML_STATE_NAME_4)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(YAML_STATE_NAME_5)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(YAML_STATE_NAME_6)).isEqualTo(DATA_TEST_STRUCTURE);
        assertThat(STATE_SERVICE.loadState(YAML_STATE_NAME_7)).isEqualTo(DATA_TEST_STRUCTURE);
    }

    @Test
    public void saveStateTest() {
        try {
            Map testStateData = JSON_MAPPER.readValue("{ \"a\": \"test\", \"b\": 1234, \"c\": [], \"d\": {} }", LinkedHashMap.class);
            boolean successful = STATE_SERVICE.saveState("testStateName", testStateData);
            assertThat(successful).isEqualTo(true);

            File testFile = new File(STATE_DIRECTORY + "/testStateName.yaml");
            assertThat(testFile.exists()).isEqualTo(true);
            Map actual = YAML_MAPPER.readValue(testFile, LinkedHashMap.class);
            assertThat(actual).isEqualTo(testStateData);

            testFile.delete();
            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException e) {
            fail(e.toString());
        }
    }

    @Test
    public void saveStateWithNoPreviousStatesTest() {
        try {
            Map testStateData = JSON_MAPPER.readValue("{ \"a\": \"test\", \"b\": 1234, \"c\": [], \"d\": {} }", LinkedHashMap.class);
            boolean successful = EMPTY_STATE_SERVICE.saveState("testStateName", testStateData);
            assertThat(successful).isEqualTo(true);

            File testFile = new File(EMPTY_STATE_DIRECTORY.toFile(), "testStateName.yaml");
            assertThat(testFile.exists()).isEqualTo(true);
            Map actual = YAML_MAPPER.readValue(testFile, LinkedHashMap.class);
            assertThat(actual).isEqualTo(testStateData);

            testFile.delete();
            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException e) {
            fail(e.toString());
        }
    }

    @Test
    public void saveStateToOverwritePreviousStateTest() {
        try {
            Map prevStateData = JSON_MAPPER.readValue("{ \"b\": [1234], \"c\": { \"d\": 5678 }, \"e\": \"prev\" }", LinkedHashMap.class);
            File testFile = new File(STATE_DIRECTORY + "/testStateName.yaml");
            JSON_MAPPER.writeValue(testFile, prevStateData);

            Map testStateData = JSON_MAPPER.readValue("{ \"a\": \"test\", \"b\": 1234, \"c\": [], \"d\": {} }", LinkedHashMap.class);
            boolean successful = STATE_SERVICE.saveState("testStateName", testStateData);
            assertThat(successful).isEqualTo(true);

            assertThat(testFile.exists()).isEqualTo(true);
            Map actual = YAML_MAPPER.readValue(testFile, LinkedHashMap.class);
            assertThat(actual).isEqualTo(testStateData);

            testFile.delete();
            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException e) {
            fail(e.toString());
        }
    }
}
