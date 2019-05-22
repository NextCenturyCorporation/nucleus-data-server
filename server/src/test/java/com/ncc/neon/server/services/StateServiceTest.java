package com.ncc.neon.server.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.ncc.neon.server.services.StateService.StateServiceFailureException;
import com.ncc.neon.server.services.StateService.StateServiceMissingFileException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = StateService.class)
public class StateServiceTest {

    private static Map DATA_NEON;
    private static Map DATA_NONE;
    private static Map DATA_NULL;
    private static Map DATA_TEST;

    private static String JSON_NEON_CONFIG = "jsonNeonConfig";
    private static String JSON_NORMAL_EXTENSION = "jsonNormalExtension";
    private static String JSON_CAPITALIZED_EXTENSION = "jsonCapitalizedExtension";
    private static String JSON_TEXT_EXTENSION = "jsonTextExtension";
    private static String JSON_CAPITALIZED_TEXT_EXTENSION = "jsonCapitalizedTextExtension";
    private static String JSON_NO_EXTENSION = "jsonNoExtension";
    private static String JSON_YAML_EXTENSION = "jsonYamlExtension";
    private static String JSON_EMPTY_OBJECT = "jsonEmptyObject";

    private static String YAML_NEON_CONFIG = "yamlNeonConfig";
    private static String YAML_NORMAL_EXTENSION = "yamlNormalExtension";
    private static String YAML_CAPITALIZED_EXTENSION = "yamlCapitalizedExtension";
    private static String YAML_ABBREVIATED_EXTENSION = "yamlAbbreviatedExtension";
    private static String YAML_CAPITALIZED_ABBREVIATED_EXTENSION = "yamlCapitalizedAbbreviatedExtension";
    private static String YAML_TEXT_EXTENSION = "yamlTextExtension";
    private static String YAML_CAPITALIZED_TEXT_EXTENSION = "yamlCapitalizedTextExtension";
    private static String YAML_NO_EXTENSION = "yamlNoExtension";
    private static String YAML_JSON_EXTENSION = "yamlJsonExtension";
    private static String YAML_EMPTY = "yamlEmpty";

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
            DATA_NULL = JSON_MAPPER.readValue("null", LinkedHashMap.class);
            DATA_NONE = JSON_MAPPER.readValue("{}", LinkedHashMap.class);
            DATA_NEON = JSON_MAPPER.readValue("{ \"dashboards\": { \"name\": \"dashboard1\" }," +
                "\"datastores\": { \"datastore1\": {} }, \"layouts\": { \"layout1\": [] }, \"options\": {} }",
                LinkedHashMap.class);
            DATA_TEST = JSON_MAPPER.readValue(
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

            STATE_SERVICE.deleteState("testStateName");

            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException | StateServiceFailureException | StateServiceMissingFileException e) {
            fail(e.toString());
        }
    }

    @Test
    public void deleteStateWithInvalidNameTest() {
        try {
            File testFile = new File(STATE_DIRECTORY + "/folder.my-test.state_name1234.yaml");
            testFile.createNewFile();
            assertThat(testFile.exists()).isEqualTo(true);

            STATE_SERVICE.deleteState("../folder/my-test.!@#$%^&*state_name~`?\\1234");

            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException | StateServiceFailureException | StateServiceMissingFileException e) {
            fail(e.toString());
        }
    }

    @Test
    public void findStateNamesTest() {
        HashSet<String> actual = new HashSet<String>(Arrays.asList(STATE_SERVICE.listStateNames()));
        assertThat(actual.contains(JSON_NEON_CONFIG)).isEqualTo(true);
        assertThat(actual.contains(JSON_NORMAL_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(JSON_CAPITALIZED_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(JSON_TEXT_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(JSON_CAPITALIZED_TEXT_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(JSON_NO_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(JSON_YAML_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(JSON_EMPTY_OBJECT)).isEqualTo(true);
        assertThat(actual.contains(YAML_NEON_CONFIG)).isEqualTo(true);
        assertThat(actual.contains(YAML_NORMAL_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_CAPITALIZED_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_ABBREVIATED_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_CAPITALIZED_ABBREVIATED_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_TEXT_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_CAPITALIZED_TEXT_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_NO_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_JSON_EXTENSION)).isEqualTo(true);
        assertThat(actual.contains(YAML_EMPTY)).isEqualTo(true);
    }

    @Test
    public void findStateNamesWithNoPreviousStatesTest() {
        assertThat(EMPTY_STATE_SERVICE.listStateNames()).isEqualTo(new String[0]);
    }

    @Test
    public void loadStateWithJsonFormatTest() {
        try {
            assertThat(STATE_SERVICE.loadState(JSON_NEON_CONFIG)).isEqualTo(DATA_NEON);
            assertThat(STATE_SERVICE.loadState(JSON_NORMAL_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(JSON_CAPITALIZED_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(JSON_TEXT_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(JSON_CAPITALIZED_TEXT_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(JSON_NO_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(JSON_YAML_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(JSON_EMPTY_OBJECT)).isEqualTo(DATA_NONE);
        }
        catch (StateServiceFailureException | StateServiceMissingFileException e) {
            fail(e.toString());
        }
    }

    @Test
    public void loadStateWithYamlFormatTest() {
        try {
            assertThat(STATE_SERVICE.loadState(YAML_NEON_CONFIG)).isEqualTo(DATA_NEON);
            assertThat(STATE_SERVICE.loadState(YAML_NORMAL_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_CAPITALIZED_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_ABBREVIATED_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_CAPITALIZED_ABBREVIATED_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_TEXT_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_CAPITALIZED_TEXT_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_NO_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_JSON_EXTENSION)).isEqualTo(DATA_TEST);
            assertThat(STATE_SERVICE.loadState(YAML_EMPTY)).isEqualTo(DATA_NULL);
        }
        catch (StateServiceFailureException | StateServiceMissingFileException e) {
            fail(e.toString());
        }
    }

    @Test
    public void saveStateTest() {
        try {
            Map testStateData = JSON_MAPPER.readValue("{ \"a\": \"test\", \"b\": 1234, \"c\": [], \"d\": {} }", LinkedHashMap.class);
            STATE_SERVICE.saveState("testStateName", testStateData);

            File testFile = new File(STATE_DIRECTORY + "/testStateName.yaml");
            assertThat(testFile.exists()).isEqualTo(true);
            Map actual = YAML_MAPPER.readValue(testFile, LinkedHashMap.class);
            assertThat(actual).isEqualTo(testStateData);

            testFile.delete();
            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException | StateServiceFailureException e) {
            fail(e.toString());
        }
    }

    @Test
    public void saveStateWithInvalidNameTest() {
        try {
            Map testStateData = JSON_MAPPER.readValue("{ \"a\": \"test\", \"b\": 1234, \"c\": [], \"d\": {} }", LinkedHashMap.class);
            STATE_SERVICE.saveState("../folder/my-test.!@#$%^&*state_name~`?\\1234", testStateData);

            File testFile = new File(STATE_DIRECTORY + "/folder.my-test.state_name1234.yaml");
            assertThat(testFile.exists()).isEqualTo(true);
            Map actual = YAML_MAPPER.readValue(testFile, LinkedHashMap.class);
            assertThat(actual).isEqualTo(testStateData);

            testFile.delete();
            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException | StateServiceFailureException e) {
            fail(e.toString());
        }
    }

    @Test
    public void saveStateWithNoPreviousStatesTest() {
        try {
            Map testStateData = JSON_MAPPER.readValue("{ \"a\": \"test\", \"b\": 1234, \"c\": [], \"d\": {} }", LinkedHashMap.class);
            EMPTY_STATE_SERVICE.saveState("testStateName", testStateData);

            File testFile = new File(EMPTY_STATE_DIRECTORY.toFile(), "testStateName.yaml");
            assertThat(testFile.exists()).isEqualTo(true);
            Map actual = YAML_MAPPER.readValue(testFile, LinkedHashMap.class);
            assertThat(actual).isEqualTo(testStateData);

            testFile.delete();
            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException | StateServiceFailureException e) {
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
            STATE_SERVICE.saveState("testStateName", testStateData);

            assertThat(testFile.exists()).isEqualTo(true);
            Map actual = YAML_MAPPER.readValue(testFile, LinkedHashMap.class);
            assertThat(actual).isEqualTo(testStateData);

            testFile.delete();
            assertThat(testFile.exists()).isEqualTo(false);
        }
        catch (IOException | StateServiceFailureException e) {
            fail(e.toString());
        }
    }
}
