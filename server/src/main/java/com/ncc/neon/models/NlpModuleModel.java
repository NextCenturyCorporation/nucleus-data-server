package com.ncc.neon.models;

import com.ncc.neon.better.HttpEndpoint;
import lombok.Data;

@Data
public class NlpModuleModel {
    private final String name;
    private final NlpModuleType type;
    private final HttpEndpoint preprocessEndpoint;
    private final HttpEndpoint listEndpoint;
    private final HttpEndpoint trainEndpoint;
    private final HttpEndpoint trainListEndpoint;
    private final HttpEndpoint infEndpoint;
    private final HttpEndpoint infListEndpoint;
}
