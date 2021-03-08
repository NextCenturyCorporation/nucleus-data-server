package com.ncc.neon.services;

import com.ncc.neon.models.IRDocEntities;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IRDocEntityService extends ElasticSearchService<IRDocEntities> {
    @Autowired
    IRDocEntityService(DatasetService datasetService,
                       @Value("${db_host}") String dbHost,
                       @Value("${ir_doc_entities.table}") String entitiesTable) {
        super(dbHost, entitiesTable, entitiesTable, IRDocEntities.class, datasetService);
    }
}
