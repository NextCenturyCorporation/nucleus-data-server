package com.ncc.neon.models;

import com.ncc.neon.util.DateUtil;
import lombok.Data;

@Data
public class Run {
    private final String configuration;
    private String[] trainOutputs;
    private String[] infOutputs;
    private String[] evalOutputs;
    private String trainStartTime = DateUtil.getCurrentDateTime();
    private String trainEndTime;
    private String infStartTime;
    private String infEndTime;
    private RunStatus status = RunStatus.TRAINING;
}
