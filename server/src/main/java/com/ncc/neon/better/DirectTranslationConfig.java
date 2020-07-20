package com.ncc.neon.better;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.DirectTranslationForm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.ncc.neon.controllers.BetterController.SHARE_PATH;

public class DirectTranslationConfig {
    public String module;

    private String name;
    private String trainFile;
    private String devFile;
    private String testFile;
    private String resultFile;
    private ArrayList<EvalConfig> evalConfigs;

    public DirectTranslationConfig(DirectTranslationForm translationForm) throws IOException {
        this.module = translationForm.getModule();
        trainFile = translationForm.getTrainFile();
        devFile = translationForm.getDevFile();
        testFile = translationForm.getTestFile();
        resultFile = translationForm.getResultFile();

        initName();
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

    public String getResultFile() {
        return resultFile;
    }

    public String getName() { return name; }

    private void initName() {
        name = "direct_translation";
    }

}
