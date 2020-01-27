package com.ncc.neon.models;

import com.ncc.neon.better.HttpEndpoint;
import lombok.Data;

@Data
public class NlpModuleModel {
    private final String _id;
    private final String name;
    private final NlpModuleType type;
    private final HttpEndpoint[] endpoints;
}
