package com.ncc.neon.services;

import com.ncc.neon.models.IRDocEntities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class IRDocEntityService extends ElasticSearchService<Map> {
    @Autowired
    IRDocEntityService(DatasetService datasetService,
                       @Value("${db_host}") String dbHost,
                       @Value("${ir_doc_entities.table}") String entitiesTable) {
        super(dbHost, entitiesTable, entitiesTable, Map.class, datasetService);
    }
}
