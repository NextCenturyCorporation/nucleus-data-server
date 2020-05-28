package com.ncc.neon.better;

import java.util.ArrayList;

public class BibertEvalConfig extends EvalConfig {
    private final String[] trainConfigParamNames = new String[] {"target_language", "method", "pretrained_model_anchor", "pretrained_model_arg", "pretrained_model_cls", "pretrained_model_head", "gpuid"};
    public BibertEvalConfig(String trainFile, String devFile, String testFile, String outputFilePrefix, ArrayList<String> bibertConfigParams) {
        super(trainFile, devFile, testFile, outputFilePrefix, bibertConfigParams);
    }

    @Override
    void initConfigParams(ArrayList<String> configParams) {
        // Populate training config params.
        for (int i = 0; i < configParams.size(); i++) {
            trainConfigParams.put(trainConfigParamNames[i], configParams.get(i));
        }

        // Populate inference config params.
        infConfigParams.put("inference_file", outputFilePrefix);
    }
}
