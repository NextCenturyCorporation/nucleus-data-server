package com.ncc.neon.adapters.sparql;

import com.ncc.neon.models.queries.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SparqlResultsConverter {

    public SparqlResultsConverter() {
    }

   public static List<Map<String, Object>> convertResults(Query query, ResultSet rset) {
        List<String> fields = query.getSelectClause().getFieldClauses().stream()
                .map(clause -> clause.getField()).collect(Collectors.toList());
        return extractHitsFromResults(fields, rset);
    }

    public static List<Map<String, Object>> extractHitsFromResults(List<String> fields, ResultSet rset) {
        List<Map<String, Object>> results = new ArrayList<>();
        try {
            while (rset.next()) {
                Map<String, Object> result = new HashMap<>();
                for (String field: fields) {
                    result.put(field, rset.getString(field));
                }
                results.add(result);
            }
            // Clean up
            rset.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return results;
    }
}
