package com.ncc.neon.better;

import java.util.*;

public class ExperimentConfig {
    private Map<String, List<String>> rawConfig;
    private Collection<ArrayList<String>> trainConfigs;
    private Object[] infConfigs;

    public ExperimentConfig(Map<String, List<String>> configMap) {
        this.rawConfig = configMap;
        parseConfig();
    }

    private void parseConfig() {
        // Extract atomic data from hash map.
        ArrayList<List<String>> values = new ArrayList<>(rawConfig.values());
        Collection<ArrayList<String>> configs = crossProduct(values);
        trainConfigs = configs;
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
