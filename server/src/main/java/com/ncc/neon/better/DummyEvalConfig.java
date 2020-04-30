package com.ncc.neon.better;

import java.util.ArrayList;

public class DummyEvalConfig extends EvalConfig {
    public DummyEvalConfig(String testFile) {
        // The dummy ie module does not need any parameters for training and inference.
        super("", "", testFile, "", new ArrayList<>());
    }

    @Override
    void initConfigParams(ArrayList<String> configParams) {
    }
}
