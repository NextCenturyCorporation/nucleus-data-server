package com.ncc.neon.better;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class ExperimentConfigTest {

    @Test
    public void testCommonConfig() {
        Map<String, List<String>> config = new HashMap<>();
        config.put("train_files", new ArrayList<>(Arrays.asList("train1.bp.json", "train2.bp.json")));
        config.put("dev_files", new ArrayList<>(Arrays.asList("dev1.bp.json", "dev2.bp.json")));
        config.put("test_files", new ArrayList<>(Arrays.asList("test1.bp.json", "test2.bp.json", "test3.bp.json")));

        try {
            ExperimentConfig experimentConfig = new ExperimentConfig(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
