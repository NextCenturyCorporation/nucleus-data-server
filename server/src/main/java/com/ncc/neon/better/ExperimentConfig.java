package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.ExperimentForm;
import com.ncc.neon.util.DateUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static com.ncc.neon.controllers.BetterController.SHARE_PATH;

public class ExperimentConfig {
    public String module;

    private String name;
    private String trainFile;
    private String devFile;
    private String testFile;
    private Map<String, List<String>> rawConfig;
    private ArrayList<EvalConfig> evalConfigs;

    public ExperimentConfig(ExperimentForm experimentForm) throws IOException {
        this.module = experimentForm.getModule();
        trainFile = experimentForm.getTrainFile();
        devFile = experimentForm.getDevFile();
        testFile = experimentForm.getTestFile();

        // Read config file.
        ObjectMapper objectMapper = new ObjectMapper();
        File configFile = new File(Paths.get(SHARE_PATH.toString(), experimentForm.getConfigFile()).toString());

        rawConfig = objectMapper.readValue(configFile, Map.class);
        evalConfigs = new ArrayList<>();

        initName();

        if (rawConfig.size() == 0) {
            // Add a single config with only required parameters.
            evalConfigs.add(EvalConfig.buildConfig(module, trainFile, devFile, testFile, name, new ArrayList<>()));
        }
        else {
            parseConfig(experimentForm.getTrainFile(), experimentForm.getDevFile());
        }
    }

    public EvalConfig[] getEvalConfigs() {
        return evalConfigs.toArray(new EvalConfig[0]);
    }

    public String getTrainFile() {
        return trainFile;
    }

    public String getDevFile() {
        return devFile;
    }

    public String getTestFile() {
        return testFile;
    }

    public String getName() { return name; }

    private void initName() {
        name = "results"; //module + "_experiment_";
    }

    private void parseConfig(String trainFile, String devFile) {
        // Extract atomic data from hash map.
        ArrayList<List<String>> values = new ArrayList<>(rawConfig.values());
        ArrayList<ArrayList<String>> configs = crossProduct(values);

        for (int i = 0; i < configs.size(); i++) {
            String outputFilePrefix = name + i;
            EvalConfig currConfig = EvalConfig.buildConfig(module, trainFile, devFile, testFile, outputFilePrefix, configs.get(i));
            evalConfigs.add(currConfig);
        }
    }

    private ArrayList<ArrayList<String>> crossProduct(ArrayList<List<String>> matrix) {
        ArrayList<ArrayList<String>> res = new ArrayList<>();

        if (matrix.size() > 0) {
            List<String> currRow = matrix.get(0);
            ArrayList<List<String>> clone = (ArrayList<List<String>>) matrix.clone();
            clone.remove(0);

            for (String item : currRow) {
                ArrayList<String> currRes = new ArrayList<>();
                ArrayList<ArrayList<String>> product = crossProduct(clone);

                if (product.size() > 0) {
                    for (ArrayList<String> elements : product) {
                        elements.add(0, item);
                        currRes = elements;
                        res.add(currRes);
                    }
                }
                else {
                    currRes.add(item);
                    res.add(currRes);
                }
            }
        }
        return res;
    }
}
