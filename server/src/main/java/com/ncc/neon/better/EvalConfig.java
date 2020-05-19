package com.ncc.neon.better;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public abstract class EvalConfig {
    private final String TRAIN_FILE_PARAM_KEY = "train_file";
    private final String DEV_FILE_PARAM_KEY = "dev_file";
    private final String TEST_FILE_PARAM_KEY = "test_file";
    private final String OUTPUT_FILE_PREFIX_PARAM_KEY = "output_file_prefix";
    private final String JOB_ID_KEY = "job_id";

    protected Map<String, String> trainConfigParams = new HashMap<>();
    protected Map<String, String> infConfigParams = new HashMap<>();
    protected String outputFilePrefix;

    abstract void initConfigParams(ArrayList<String> configParams);

    public static EvalConfig buildConfig(String module, String trainFile, String devFile, String testFile, String outputFilePrefix, ArrayList<String> configParams) {
        switch (module) {
            case "mbert":
                return new MbertEvalConfig(trainFile, devFile, testFile, outputFilePrefix, configParams);
            default:
                return new DummyEvalConfig(trainFile, devFile);
        }
    }

    public EvalConfig(String trainFile, String devFile, String testFile, String outputFilePrefix, ArrayList<String> configParams) {
        this.outputFilePrefix = outputFilePrefix;

        trainConfigParams.put(TRAIN_FILE_PARAM_KEY, trainFile);
        trainConfigParams.put(DEV_FILE_PARAM_KEY, devFile);
        trainConfigParams.put(OUTPUT_FILE_PREFIX_PARAM_KEY, this.outputFilePrefix);
        infConfigParams.put(TEST_FILE_PARAM_KEY, testFile);
        
        initConfigParams(configParams);
    }

    public Map<String, String> getTrainConfigParams() {
        return trainConfigParams;
    }

    public Map<String, String> getInfConfigParams() {
        return infConfigParams;
    }

    public void setJobIdParam(String jobId) {
        trainConfigParams.put(JOB_ID_KEY, jobId);
    }
}
