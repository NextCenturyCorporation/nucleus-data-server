package com.ncc.neon.better;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

import com.ncc.neon.models.ExperimentForm;

public class ExperimentConfigTest {

    @Test
    public void testCommonConfig() {
        ExperimentForm experimentForm = new ExperimentForm("train1.bp.json", "dev1.bp.json", "test1.bp.json", "config.json", "dummy", true);

        try {
            ExperimentConfig experimentConfig = new ExperimentConfig(experimentForm);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
