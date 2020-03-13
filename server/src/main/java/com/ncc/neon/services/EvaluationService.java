package com.ncc.neon.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ncc.neon.models.EvaluationOutput;
import com.ncc.neon.models.Score;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.ArrayList;

@Component
public class EvaluationService extends ElasticSearchService<EvaluationOutput> {
    @Autowired
    EvaluationService(DatasetService datasetService,
                      @Value("${db_host}") String dbHost,
                      @Value("${evaluation.table}") String evalTable) {
        super(dbHost, evalTable, evalTable, EvaluationOutput.class, datasetService);
    }

    public Mono<?> getOverallScoreByRunId(String runId) {
        SearchRequest searchRequest = new SearchRequest("evaluation");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder queryBuilder = QueryBuilders
                .matchQuery("runID", runId);

        searchSourceBuilder.query(queryBuilder);

        return Mono.create(sink -> {
           searchRequest.source(searchSourceBuilder);
            try {
                SearchResponse response = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);

                ObjectMapper objectMapper = new ObjectMapper();
                EvaluationOutput evaluationOutput = objectMapper.convertValue(response.getHits().getAt(0).getSourceAsMap(), EvaluationOutput.class);

                sink.success(evaluationOutput.getScore()[evaluationOutput.getScore().length - 1]);
            } catch (IOException e) {
                sink.error(e);
            }
        });
    }
}
