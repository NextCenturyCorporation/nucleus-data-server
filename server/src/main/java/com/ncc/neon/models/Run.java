package com.ncc.neon.models;

import com.ncc.neon.util.DateUtil;
import lombok.Data;

@Data
public class Run {
    private final String train_config;
    private final String inf_config;
    private String[] train_outputs;
    private String[] inf_outputs;
    private String[] eval_outputs;
    private String train_start_time;
    private String train_end_time;
    private String inf_start_time;
    private String inf_end_time;
    private RunStatus status;
    private String status_message;
}
