package com.ncc.neon.server.models.datasource;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@JsonTest
public class DataConfigTests {

    private static final String TAGS_FIELD = "estest.user-index.data.firstname";

    @Autowired
    private JacksonTester<DataConfig> json;

    @Test
    public void testSerializationAndDeserialization() throws Exception {
        DataConfig sampleConfig = this.json.read("/json/testDataConfig.json").getObject();
        assertThat(this.json.write(sampleConfig)).isEqualToJson("/json/testDataConfig.json");
    }

    @Test
    public void testGettingFields() throws Exception {
        DataConfig sampleConfig = this.json.read("/json/testDataConfig.json").getObject();
        sampleConfig.build();

        // Try getting all the parts from TAGS_FIELD
        DataStore dataStore = sampleConfig.getDataStore(TAGS_FIELD);
        assertThat(dataStore).isNotNull();
        assertThat(TAGS_FIELD).startsWith(dataStore.getName());

        Database database = sampleConfig.getDatabase(TAGS_FIELD);
        assertThat(database).isNotNull();
        assertThat(TAGS_FIELD).contains(database.getName());

        Table table = sampleConfig.getTable(TAGS_FIELD);
        assertThat(table).isNotNull();
        assertThat(TAGS_FIELD).contains(table.getName());

        Field field = sampleConfig.getField(TAGS_FIELD);
        assertThat(field).isNotNull();
        assertThat(TAGS_FIELD).isEqualTo(field.toString());
    }
}
