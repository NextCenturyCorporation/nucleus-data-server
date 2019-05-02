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
    private List<DataConfig> dataConfigs;
    private DataConfig dataConfig;
    private QueryService queryService;

    DataConfigService(QueryService queryService) {
        this.queryService = queryService;
    }

    // The default value is prefixed with ../ for now so it is in the root directory when running with Gradle
    @Value("${dataStoreConfigDir:../dataStore}")
    private String dataStoreConfigDir;

    public List<DataConfig> getDataConfigs() {
        return this.dataConfigs;
    }

    public DataConfig getDataConfig() {
        return this.dataConfig;
    }

    @PostConstruct
    public void init() {
        dataConfigs = new ArrayList<>();

        log.debug("Looking for data set config files");
        log.debug("dataStoreConfigDir: '" + dataStoreConfigDir + "'");

        try {
            File configDir = ResourceUtils.getFile(dataStoreConfigDir);
            log.debug("dataStoreConfig file path: " + configDir.getAbsolutePath());

            if (!configDir.isDirectory()) {
                throw new IllegalArgumentException("dataStoreConfigDir is not a directory");
            }
            // Get possible config files
            File[] configFiles = configDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".json") ||
                    name.toLowerCase().endsWith(".yml") || name.toLowerCase().endsWith(".yaml"));

            if (configFiles == null) {
                throw new Exception("No data store config files found");
            }

            ObjectMapper mapper = new ObjectMapper();
            Yaml yamlReader = new Yaml(new Constructor(DataConfig.class));

            for (File configFile : configFiles) {
                try {
                    log.debug("Loading data store config file: " + configFile.getName());
                    DataConfig dataSet;
                    if (configFile.getName().toLowerCase().endsWith(".json")) {
                        dataSet = mapper.readValue(configFile, DataConfig.class);
                    } else {
                        dataSet = yamlReader.load(new FileReader(configFile));
                    }

                    dataConfigs.add(dataSet);
                } catch (IOException e) {
                    log.error("Exception reading config file", e);
                }
            }

            log.debug("Loaded " + dataConfigs.size() + " config files");
        } catch (FileNotFoundException e) {
            log.error("Error opening dataStoreConfigDir", e);
        } catch (Exception e) {
            log.error("Exception loading data store configs", e);
        }

        // TODO - Merge configs

        // Fetch missing field data
        fetchFieldInfo();

        dataConfig = dataConfigs.get(0);
        dataConfig.build();
    }

    /**
     * Fetch the field names and data types
     */
    private void fetchFieldInfo() {
        log.debug("Starting to fetch field names");

        for (DataConfig dataConfig : dataConfigs) {
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

}
