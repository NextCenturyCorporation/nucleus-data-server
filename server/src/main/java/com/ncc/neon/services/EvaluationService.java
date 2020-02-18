package com.ncc.neon.services;

import com.ncc.neon.models.EvaluationOutput;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EvaluationService extends ElasticSearchService<EvaluationOutput> {
    @Autowired
    EvaluationService(DatasetService datasetService,
                      @Value("${db_host}") String dbHost,
                      @Value("${evaluation.table}") String evalTable) {
        super(dbHost, evalTable, evalTable, EvaluationOutput.class, datasetService);
    }
}
