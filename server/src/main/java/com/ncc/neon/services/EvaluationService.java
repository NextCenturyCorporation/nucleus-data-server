package com.ncc.neon.services;

import com.ncc.neon.models.EvaluationOutput;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EvaluationService extends ElasticSearchService<EvaluationOutput> {
    private static final String index = "evaluation";
    private static final String dataType = "evaluation";

    @Autowired
    EvaluationService(DatasetService datasetService, @Value("${db_host}") String dbHost) {
        super(dbHost, index, dataType, EvaluationOutput.class, datasetService);
    }
}
