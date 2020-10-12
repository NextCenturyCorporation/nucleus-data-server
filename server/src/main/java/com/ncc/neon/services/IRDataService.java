package com.ncc.neon.services;

import com.ncc.neon.models.Docfile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;


@Component
public class IRDataService extends ElasticSearchService<Docfile> {
    @Autowired
    private DatasetService datasetService;

    @Autowired
    IRDataService(DatasetService datasetService,
                  @Value("${db_host}") String dbHost,
                  @Value("${file.table}") String fileTable) {
        super(dbHost, fileTable, fileTable, Docfile.class, datasetService);
    }

    public ArrayList<Docfile> getIRDocResponse(String index, String type, String[] searchIDs) throws IOException {
        ArrayList<Docfile> respondList = new ArrayList<Docfile>();
        for (String id : searchIDs) {
            respondList.add(this.getByDocId(index, type, id));
        }
        return respondList;
    }

}
