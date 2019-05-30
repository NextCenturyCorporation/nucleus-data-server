package com.ncc.neon.server.services;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.ncc.neon.server.models.results.PagedList;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class StateService {

    @Value("${state.directory:../states}")
    private String stateDirectoryPath;

    private static ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    public class StateServiceFailureException extends Exception {
        private static final long serialVersionUID = 1L;

        StateServiceFailureException(String s) {
            super(s);
        }
    }

    public class StateServiceMissingFileException extends Exception {
        private static final long serialVersionUID = 1L;

        StateServiceMissingFileException(String s) {
            super(s);
        }
    }

    StateService() {
        this(null);
    }

    StateService(String stateDirectoryPath) {
        if (stateDirectoryPath != null) {
            this.stateDirectoryPath = stateDirectoryPath;
            log.debug("Configured State Directory Path = " + this.stateDirectoryPath);
        }
        JSON_MAPPER.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        YAML_MAPPER.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        YAML_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * Deletes the state with the given name.
     *
     * @param stateName
     * @throws StateServiceFailureException
     * @throws StateServiceMissingFileException
     */
    public void deleteState(String stateName) throws StateServiceFailureException, StateServiceMissingFileException {
        File stateDirectory = findStateDirectory();
        File stateFile = retrieveStateFile(stateDirectory, this.validateName(stateName));
        if(stateFile == null) {
            throw new StateServiceMissingFileException("State " + stateName + " does not exist");
        }
        stateFile.delete();
        if(stateFile.exists()) {
            log.error("Cannot delete state from " + stateFile.getAbsolutePath());
            throw new StateServiceFailureException("State " + stateName + " was not deleted");
        }
    }

    /**
     * Returns the state directory object.
     * 
     * @return File
     */
    private File findStateDirectory() {
        try {
            File stateDirectory = ResourceUtils.getFile(stateDirectoryPath);
            log.debug("State Directory Path = " + stateDirectory.getAbsolutePath());
            if(!stateDirectory.exists()) {
                stateDirectory.mkdir();
            }
            if(!stateDirectory.isDirectory()) {
                throw new IllegalArgumentException("stateDirectoryPath is not a directory!");
            }
            return stateDirectory;
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Cannot create state directory!");
        }
    }

    /**
     * Returns the array of saved states given a limit and an offset
     *
     * @return Array
     */
    public PagedList<Map> listStates(int limit, int offset) {
        File stateDirectory = findStateDirectory();
        List<File> files = Arrays.asList(stateDirectory.listFiles());

        if (files == null) {
            return new PagedList<>(new Map[0], 0);
        }

        int total = files.size();

        if (total == 0) {
            return new PagedList<>(new Map[0], 0);
        }

        Collections.sort(files, (File a, File b) -> {
            int ret = Long.compare(b.lastModified(), a.lastModified());
            return ret != 0 ? ret : a.getName().compareTo(b.getName());
        });

        int maxEnd = total - 1;

        if (offset > maxEnd) {
            return new PagedList<>(new Map[0], 0);
        } else if (offset + limit > maxEnd) {
            limit = total - offset;
        }

        List<File> finalList = files.subList(offset, offset + limit);

        Map[] results = finalList
            .stream()            
            .map((f) -> {
                try {
                    String fileName = f.getName();
                    return this.loadState(fileName, true);
                } catch (StateServiceFailureException | StateServiceMissingFileException e) {
                    log.error("Unable to load config " + f + " due to " + e.getMessage());
                    return null;
                }
            })
            .filter(v -> v != null)
            .toArray(sz -> new Map[sz]);

        return new PagedList<>(results, total);
    }

    /**
     * Returns the data (with no specific format) in the state with the given name, or an empty map if no state exists.
     *
     * @param stateName
     * @return Map
     * @throws StateServiceFailureException
     * @throws StateServiceMissingFileException
     */
    public Map loadState(String stateName, boolean annotate) throws StateServiceFailureException, StateServiceMissingFileException {
        File stateDirectory = findStateDirectory();
        File stateFile = retrieveStateFile(stateDirectory, this.validateName(stateName));
        if (stateFile == null) {
            throw new StateServiceMissingFileException("State " + stateName + " does not exist");
        }

        Map config;
        try {
            config = JSON_MAPPER.readValue(stateFile, LinkedHashMap.class);
        }
        catch (IOException jsonException) {
            try {
                config = YAML_MAPPER.readValue(stateFile, LinkedHashMap.class);
            }
            catch (IOException yamlException) {
                log.error("Cannot load state from " + stateFile.getAbsolutePath());
                throw new StateServiceFailureException("State " + stateName + " is not JSON or YAML");
            }
        }
        if (config != null && annotate) {
            config.put("fileName", stateName);
            config.put("lastModified", stateFile.lastModified());
        }
        return config;
    }

    /**
     * Returns the state file object of the state with the given name in the given directory, or null if no file exists.
     *
     * @param stateDirectory
     * @param stateName
     * @return File
     */
    private File retrieveStateFile(File stateDirectory, String stateName) {
        String validName = this.validateName(stateName);
        File[] stateFiles = stateDirectory.listFiles((directory, name) -> name.equals(validName) ||
            name.equals(validName + ".json") || name.equals(validName + ".JSON") || name.equals(validName + ".yaml") ||
            name.equals(validName + ".YAML") || name.equals(validName + ".yml") || name.equals(validName + ".YML") ||
            name.equals(validName + ".txt") || name.equals(validName + ".TXT"));
        return stateFiles.length > 0 ? stateFiles[0] : null;
    }

    /**
     * Saves the given data (with no specific format) as a state with the given name.
     *
     * @param stateName
     * @param stateData
     * @throws StateServiceFailureException
     */
    public void saveState(String stateName, Map stateData) throws StateServiceFailureException {
        File stateDirectory = findStateDirectory();
        log.debug("Save State " + this.validateName(stateName) + " : \n" + stateData.toString());
        File stateFile = new File(stateDirectory, this.validateName(stateName) + ".yaml");
        try {
            YAML_MAPPER.writeValue(stateFile, stateData);
        }
        catch (IOException e) {
            e.printStackTrace();
            log.error("Cannot save state to " + stateFile.getAbsolutePath());
            throw new StateServiceFailureException("State " + stateName + " was not saved");
        }
    }

    private String validateName(String stateName) {
        // Replace / with . and remove ../ and non-alphanumeric characters except ._-+=,
        return stateName.replaceAll("\\.\\./", "").replaceAll("/", ".").replaceAll("[^A-Za-z0-9\\.\\_\\-\\+\\=\\,]", "");
    }
}
