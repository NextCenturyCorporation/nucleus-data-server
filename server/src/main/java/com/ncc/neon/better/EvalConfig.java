package com.ncc.neon.better;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class EvalConfig {
    private final String TRAIN_FILE_PARAM_KEY = "train_file";
    private final String DEV_FILE_PARAM_KEY = "dev_file";
    private final String OUTPUT_FILE_PREFIX_PARAM_KEY = "output_file_prefix";

    protected Map<String, String> trainConfigParams = new HashMap<>();
    protected Map<String, String> infConfigParams = new HashMap<>();

    private String trainFile;
    private String devFile;
    private String testFile;
    private String trainConfigFilename;
    private String infConfigFilename;
    protected String outputFilePrefix;

    abstract void initConfigParams(ArrayList<String> configParams);

    public static EvalConfig buildConfig(String module, String trainFile, String devFile, String testFile, String outputFilePrefix, ArrayList<String> configParams) {
        switch (module) {
            case "mbert":
                return new MbertEvalConfig(trainFile, devFile, testFile, outputFilePrefix, configParams);
            default:
                return new DummyEvalConfig(testFile);
        }
    }

    public EvalConfig(String trainFile, String devFile, String testFile, String outputFilePrefix, ArrayList<String> configParams) {
        this.trainFile = trainFile;
        this.devFile = devFile;
        this.testFile = testFile;
        this.outputFilePrefix = outputFilePrefix;
        trainConfigFilename = outputFilePrefix + "_train_config.json";
        infConfigFilename = outputFilePrefix + "_inf_config.json";

        trainConfigParams.put(TRAIN_FILE_PARAM_KEY, trainFile);
        trainConfigParams.put(DEV_FILE_PARAM_KEY, devFile);
        trainConfigParams.put(OUTPUT_FILE_PREFIX_PARAM_KEY, this.outputFilePrefix);

        initConfigParams(configParams);

        // TODO: Add train, dev, and test config params.

        // TODO: Write config files to share.
    }

    public String getTrainConfigFilename() {
        return trainConfigFilename;
    }

    public String getInfConfigFilename() {
        return infConfigFilename;
    }

    private void writeToShare() {

    }
}
