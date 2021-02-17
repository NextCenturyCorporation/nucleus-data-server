package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.IRExperimentForm;
import com.ncc.neon.util.DateUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.ncc.neon.controllers.BetterController.SHARE_PATH;

public class IRExperimentConfig {
    public String module;

    private String name;
    private String taskFile;
    private String corpus;
    private Map<String, List<String>> rawConfig;
    private ArrayList<EvalConfig> evalConfigs;

    public IRExperimentConfig(IRExperimentForm experimentForm) throws IOException {
        this.module = experimentForm.getModule();
        taskFile = experimentForm.getTaskFile();
        corpus = experimentForm.getCorpus();
        name = module + "_experiment_" + DateUtil.getCurrentDateTime();

        // Read config file.
        ObjectMapper objectMapper = new ObjectMapper();
        File configFile = new File(Paths.get(SHARE_PATH.toString(), experimentForm.getConfigFile()).toString());

        rawConfig = objectMapper.readValue(configFile, Map.class);
        evalConfigs = new ArrayList<>();

        if (rawConfig.size() == 0) {
            // Add a single config with only required parameters.
            evalConfigs.add(EvalConfig.buildConfig(module, taskFile, corpus, name, new ArrayList<>()));
        }
        else {
            parseConfig(experimentForm.getTaskFile(), experimentForm.getCorpus());
        }
    }

    public EvalConfig[] getEvalConfigs() {
        return evalConfigs.toArray(new EvalConfig[0]);
    }

    public String getTaskFile() {
        return taskFile;
    }

    public String getCorpus() {
        return corpus;
    }


    public String getName() { return name; }

    private void initName() {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
        name = module + "_experiment_" + df.format(new Date());
    }

    private void parseConfig(String trainFile, String devFile) {
        // Extract atomic data from hash map.
        ArrayList<List<String>> values = new ArrayList<>(rawConfig.values());
        ArrayList<ArrayList<String>> configs = crossProduct(values);

        for (int i = 0; i < configs.size(); i++) {
            String outputFilePrefix = name + i;
            EvalConfig currConfig = EvalConfig.buildConfig(module, trainFile, devFile, outputFilePrefix, configs.get(i));
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