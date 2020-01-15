package com.ncc.neon.better;

import com.ncc.neon.services.BetterFileService;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.FileShareService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IENlpModule extends NlpModule {
    private HttpEndpoint trainEndpoint;
    private HttpEndpoint trainListEndpoint;
    private HttpEndpoint infEndpoint;
    private HttpEndpoint infListEndpoint;

    @Autowired
    IENlpModule(DatasetService datasetService, FileShareService fileShareService, BetterFileService betterFileService) {
        super(datasetService, fileShareService, betterFileService);
    }

    public void setTrainEndpoint(HttpEndpoint trainEndpoint) {
        this.trainEndpoint = trainEndpoint;
    }

    public void setTrainListEndpoint(HttpEndpoint trainListEndpoint) {
        this.trainListEndpoint = trainListEndpoint;
    }

    public void setInfEndpoint(HttpEndpoint infEndpoint) {
        this.infEndpoint = infEndpoint;
    }

    public void setInfListEndpoint(HttpEndpoint infListEndpoint) {
        this.infListEndpoint = infListEndpoint;
    }
}
