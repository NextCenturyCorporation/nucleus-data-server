package com.ncc.neon.better;

import java.util.ArrayList;

public class MbertEvalConfig extends EvalConfig {
    private final String[] trainConfigParamNames = new String[] {"seed", "batch_size", "bert_model", "learning_rate", "max_epoch", "gpuid", "max_seq_len", "warmup_proportion", "gradient_accumulation_steps"};
    public MbertEvalConfig(String trainFile, String devFile, String outputFilePrefix, ArrayList<String> mbertConfigParams) {
        super(trainFile, devFile, outputFilePrefix, mbertConfigParams);
    }

    @Override
    void initConfigParams(ArrayList<String> configParams) {
        // Populate training config params.
        for (int i = 0; i < configParams.size(); i++) {
            trainConfigParams.put(trainConfigParamNames[i], configParams.get(i));
        }

        // Populate inference config params.
        infConfigParams.put("ckpt_task1", outputFilePrefix + "_T1");
        infConfigParams.put("ckpt_task2", outputFilePrefix + "_T2");
        infConfigParams.put("ckpt_task3", outputFilePrefix + "_T3");
        infConfigParams.put("ckpt_task4", outputFilePrefix + "_T4");
        infConfigParams.put("inf_file", outputFilePrefix);
    }
}
