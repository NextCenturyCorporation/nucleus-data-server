package com.ncc.neon.server.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.server.models.datasource.DataSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class DataSetService {
    private List<DataSet> dataSets;

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
    }
}
