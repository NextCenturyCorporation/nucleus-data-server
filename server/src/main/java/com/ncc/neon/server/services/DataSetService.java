package com.ncc.neon.server.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.server.models.ConnectionInfo;
import com.ncc.neon.server.models.datasource.DataSet;
import com.ncc.neon.server.models.datasource.Database;
import com.ncc.neon.server.models.datasource.Field;
import com.ncc.neon.server.models.datasource.Table;
import com.ncc.neon.server.models.results.FieldTypePair;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

@Slf4j
@Component
public class DataSetService {
    private List<DataSet> dataSets;
    private QueryService queryService;

    DataSetService(QueryService queryService) {
        this.queryService = queryService;
    }

    // The default value is prefixed with ../ for now so it is in the root directory when running with Gradle
    @Value("${dataStoreConfigDir:../dataStore}")
    private String dataStoreConfigDir;

    public List<DataSet> getDataSets() {
        return this.dataSets;
    }

    @PostConstruct
    public void init() {
        dataSets = new ArrayList<>();

        log.debug("Looking for data set config files");
        log.debug("dataStoreConfigDir: '" + dataStoreConfigDir + "'");

        try {
            File configDir = ResourceUtils.getFile(dataStoreConfigDir);
            log.debug("dataStoreConfig file path: " + configDir.getAbsolutePath());

            if (!configDir.isDirectory()) {
                throw new IllegalArgumentException("dataStoreConfigDir is not a directory");
            }
            // Get possible config files
            File[] configFiles = configDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".json"));

            if (configFiles == null) {
                throw new Exception("No data store config files found");
            }

            ObjectMapper mapper = new ObjectMapper();

            for (File configFile : configFiles) {
                try {
                    log.debug("Loading data store config file: " + configFile.getName());
                    DataSet dataSet = mapper.readValue(configFile, DataSet.class);
                    dataSets.add(dataSet);
                } catch (IOException e) {
                    log.error("Exception reading config file", e);
                }
            }

            log.debug("Loaded " + dataSets.size() + " config files");
        } catch (FileNotFoundException e) {
            log.error("Error opening dataStoreConfigDir", e);
        } catch (Exception e) {
            log.error("Exception loading data store configs", e);
        }

        fetchFieldInfo();
    }

    /**
     * Fetch the field names and data types
     */
    private void fetchFieldInfo() {
        log.debug("Starting to fetch field names");

        for (DataSet dataSet : dataSets) {
            ConnectionInfo connectionInfo = new ConnectionInfo(dataSet.getDatastore(), dataSet.getHostname());
            for (Database database : dataSet.getDatabases()) {
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
                            newField = new Field(field.getField(), field.getField(), field.getType());
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
