package com.ncc.neon.server.services;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

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
     * @return Boolean of whether the action was successful.
     */
    public boolean deleteState(String stateName) {
        File stateDirectory = findStateDirectory();
        File stateFile = findStateFile(stateDirectory, this.validateName(stateName));
        if (stateFile == null) {
            return true;
        }
        stateFile.delete();
        return !stateFile.exists();
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
     * Returns the state file object of the state with the given name in the given directory, or null if no file exists.
     *
     * @param stateDirectory
     * @param stateName
     * @return File
     */
    private File findStateFile(File stateDirectory, String stateName) {
        String validName = this.validateName(stateName);
        File[] stateFiles = stateDirectory.listFiles((directory, name) -> name.equals(validName) ||
            name.equals(validName + ".json") || name.equals(validName + ".JSON") || name.equals(validName + ".yaml") ||
            name.equals(validName + ".YAML") || name.equals(validName + ".yml") || name.equals(validName + ".YML") ||
            name.equals(validName + ".txt") || name.equals(validName + ".TXT"));
        return stateFiles.length > 0 ? stateFiles[0] : null;
    }

    /**
     * Returns the array of saved state names.
     *
     * @return Array
     */
    public String[] findStateNames() {
        File stateDirectory = findStateDirectory();
        File[] files = stateDirectory.listFiles();
        Arrays.sort(files != null ? files : new File[0]);
        List<String> names = Arrays.stream(files).map(file -> {
            String name = file.getName();
            int extensionIndex = name.lastIndexOf('.');
            return extensionIndex < 0 ? name : name.substring(0, extensionIndex);
        }).collect(Collectors.toList());
        return names.toArray(new String[names.size()]);
    }

    /**
     * Returns the data in the state with the given name, or an empty map if no state exists.
     *
     * @param stateName
     * @return Map
     */
    public Map loadState(String stateName) {
        File stateDirectory = findStateDirectory();
        File stateFile = findStateFile(stateDirectory, this.validateName(stateName));
        if (stateFile == null) {
            return new LinkedHashMap();
        }
        try {
            return JSON_MAPPER.readValue(stateFile, LinkedHashMap.class);
        }
        catch (IOException jsonException) {
            try {
                return YAML_MAPPER.readValue(stateFile, LinkedHashMap.class);
            }
            catch (IOException yamlException) {
                log.error("Cannot load state from " + stateFile.getAbsolutePath());
                return new LinkedHashMap();
            }
        }
    }

    /**
     * Saves the given data as a state with the given name.
     *
     * @param stateName
     * @param stateData
     * @return Boolean of whether the action was successful.
     */
    public boolean saveState(String stateName, Map stateData) {
        File stateDirectory = findStateDirectory();
        log.debug("Save State " + this.validateName(stateName) + " : \n" + stateData.toString());
        File stateFile = new File(stateDirectory, this.validateName(stateName) + ".yaml");
        try {
            YAML_MAPPER.writeValue(stateFile, stateData);
            return true;
        }
        catch (JsonGenerationException e) {
            e.printStackTrace();
        }
        catch (JsonMappingException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private String validateName(String stateName) {
        // Replace ../ with . and remove all non-alphanumeric characters except (._-+=,) (but not the parentheses).
        return stateName.replaceAll("(\\.\\.)?/", ".").replaceAll("[^A-Za-z0-9\\.\\_\\-\\+\\=\\,]", "");
    }
}
