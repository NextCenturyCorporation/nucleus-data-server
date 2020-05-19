package com.ncc.neon.better;

import java.util.ArrayList;

public class DummyEvalConfig extends EvalConfig {
    public DummyEvalConfig(String trainFile, String devFile) {
        // The dummy ie module does not need any parameters for training and inference.
        super(trainFile, devFile, "", "dummy", new ArrayList<>());
    }

    @Override
    void initConfigParams(ArrayList<String> configParams) {
    }
}
