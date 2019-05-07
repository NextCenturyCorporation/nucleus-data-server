package com.ncc.neon.server.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.server.models.ConnectionInfo;
import com.ncc.neon.server.models.datasource.*;
import com.ncc.neon.server.models.results.FieldTypePair;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

@Slf4j
@Component
public class DataConfigService {
    private DataConfig dataConfig;
    private QueryService queryService;

    DataConfigService(QueryService queryService) {
        this.queryService = queryService;
    }

    // The default value is prefixed with ../ for now so it is in the root directory when running with Gradle
    @Value("${dataStoreConfig:../sampleDataConfig.json}")
    private String dataStoreConfigFile;

    public DataConfig getDataConfig() {
        return this.dataConfig;
    }

    @PostConstruct
    public void init() {
        log.debug("Looking for data set config file");
        log.debug("dataStoreConfigFile: '" + dataStoreConfigFile + "'");

        try {
            File configFile = ResourceUtils.getFile(dataStoreConfigFile);
            log.debug("dataStoreConfig file path: " + configFile.getAbsolutePath());

            if (!configFile.exists()) {
                throw new IllegalArgumentException("dataStoreConfigFile does nto exist");
            }
            if (configFile.isDirectory()) {
                throw new IllegalArgumentException("dataStoreConfigFile is a directory");
            }

            // Config can be JSON or YAML
            ObjectMapper mapper = new ObjectMapper();
            Yaml yamlReader = new Yaml(new Constructor(DataConfig.class));

            try {
                // Pick the reader based on extension
                if (configFile.getName().toLowerCase().endsWith(".json")) {
                    dataConfig = mapper.readValue(configFile, DataConfig.class);
                } else if (configFile.getName().toLowerCase().endsWith(".yaml") ||
                        configFile.getName().toLowerCase().endsWith(".yml")) {
                    dataConfig = yamlReader.load(new FileReader(configFile));
                }
            } catch (IOException e) {
                log.error("Exception reading config file", e);
            }

            if (dataConfig == null ) {
                throw new Exception("Error reading data config");
            } else {
                log.debug("Loaded config file");
            }

        } catch (FileNotFoundException e) {
            log.error("Error opening dataStoreConfigFile", e);
        } catch (Exception e) {
            log.error("Exception loading data store configs", e);
        }

        // Fetch missing field data
        fetchFieldInfo();

        dataConfig.build();
    }

    /**
     * Fetch the field names and data types
     */
    private void fetchFieldInfo() {
        log.debug("Starting to fetch field names");

        for (DataStore dataStore : dataConfig.getDataStores()) {
            ConnectionInfo connectionInfo = new ConnectionInfo(dataStore.getType(), dataStore.getHostname());
            for (Database database : dataStore.getDatabases()) {
                for (Table table : database.getTables()) {
                    Map<String, Field> fieldMap = new HashMap<>();

                    // Build a map of columnName to field object to merge config fields with the fields pulled from the DB
                    for (Field field : table.getFields()) {
                        fieldMap.put(field.getColumnName(), field);
                    }

                    List<FieldTypePair> fieldTypePairs = queryService.getFieldTypes(connectionInfo, database.getName(), table.getName()).collectList().block();
                    // Make sure there are fields
                    if (fieldTypePairs == null || fieldTypePairs.size() == 0) {
                        log.debug("No fields found for table " + table.getName());
                        continue;
                    }

                    log.debug("Fields for table " + table.getName());

                    for (FieldTypePair field : fieldTypePairs) {
                        log.debug(field.getField() + " - " + field.getType());
                        Field newField = fieldMap.get(field.getField());
                        if (newField == null) {
                            newField = new Field(field.getField(), field.getField(), field.getType(), table);
                        } else {
                            // Set possibly null data
                            if (newField.getType() == null) {
                                newField.setType(field.getType());
                            }
                            if (newField.getPrettyName() == null) {
                                newField.setPrettyName(field.getField());
                            }
                        }

                        fieldMap.put(newField.getColumnName(), newField);
                    }
                    table.setFields(fieldMap.values().toArray(new Field[0]));

                    // Sort the fields
                    Arrays.sort(table.getFields(), (Field a, Field b) ->
                            a.getColumnName().compareTo(b.getColumnName()));
                }
            }
        }
    }

}
