package com.ncc.neon.services;

import com.ncc.neon.models.EvaluationOutput;
import com.ncc.neon.models.Score;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class EvaluationService extends ElasticSearchService<EvaluationOutput> {
    @Autowired
    EvaluationService(DatasetService datasetService,
                      @Value("${db_host}") String dbHost,
                      @Value("${evaluation.table}") String evalTable) {
        super(dbHost, evalTable, evalTable, EvaluationOutput.class, datasetService);
    }

    public Mono<Score> getOverallScore(String evalId) {
        // The overall score is the last score in the evaluation.
        return getById(evalId)
                .map(eval -> eval.getScore()[eval.getScore().length - 1]);
    }
}
