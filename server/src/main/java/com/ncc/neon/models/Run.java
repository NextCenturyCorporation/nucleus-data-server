package com.ncc.neon.models;

import com.ncc.neon.util.DateUtil;
import lombok.Data;

@Data
public class Run {
    private final String configuration;
    private String[] train_outputs;
    private String[] inf_outputs;
    private String[] eval_outputs;
    private String train_start_time = DateUtil.getCurrentDateTime();
    private String train_end_time;
    private String inf_start_time;
    private String inf_end_time;
    private RunStatus status = RunStatus.TRAINING;
    private String status_message;
}
