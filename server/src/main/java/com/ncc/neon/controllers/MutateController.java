package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.results.ActionResult;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.QueryService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("mutateservice")
public class MutateController {
    private DatasetService datasetService;
    private QueryService queryService;

    MutateController(DatasetService datasetService, QueryService queryService) {
        this.datasetService = datasetService;
        this.queryService = queryService;
    }

    @PostMapping(path="/byid", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Mono<ActionResult>> mutateDataById(@RequestBody MutateQuery mutateQuery) {
        final Predicate<String> isBlank = StringUtils::isBlank;
        final Predicate<Collection> isEmpty = CollectionUtils::isEmpty;

        Triple[] labeledInput = new Triple[]{
            Triple.of("Datastore Host", mutateQuery.getDatastoreHost(), isBlank),
            Triple.of("Datastore Type", mutateQuery.getDatastoreType(), isBlank),
            Triple.of("Database Name", mutateQuery.getDatabaseName(), isBlank),
            Triple.of("Table Name", mutateQuery.getTableName(), isBlank),
            Triple.of("ID Field", mutateQuery.getIdFieldName(), isBlank),
            Triple.of("Data ID", mutateQuery.getDataId(), isBlank),
            Triple.of("Fields with Values", mutateQuery.getFieldsWithValues().entrySet(), isEmpty),
        };

        String mutationErrorString = "Mutation by ID Query Missing ";

        return mutateData(labeledInput, mutateQuery, mutationErrorString, false);
    }

    @PostMapping(path="/byfilter", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Mono<ActionResult>> mutateDataByFilter(@RequestBody MutateQuery mutateQuery) {
        final Predicate<String> isBlank = StringUtils::isBlank;
        final Predicate<Collection> isEmpty = CollectionUtils::isEmpty;
        final Predicate<Object> isNull = Objects::isNull;

        Triple[] labeledInput = new Triple[]{
            Triple.of("Datastore Host", mutateQuery.getDatastoreHost(), isBlank),
            Triple.of("Datastore Type", mutateQuery.getDatastoreType(), isBlank),
            Triple.of("Database Name", mutateQuery.getDatabaseName(), isBlank),
            Triple.of("Table Name", mutateQuery.getTableName(), isBlank),
            Triple.of("Where Clause", mutateQuery.getWhereClause(), isNull),
            Triple.of("Fields with Values", mutateQuery.getFieldsWithValues().entrySet(), isEmpty)
        };

        String mutationErrorString = "Mutation by Filter Query Missing ";

        return mutateData(labeledInput, mutateQuery, mutationErrorString, true);
    }

    private ResponseEntity<Mono<ActionResult>> mutateData(Triple[] labeledInput, @RequestBody MutateQuery mutateQuery,
                                                          String mutationErrorString, Boolean checkWhereClause) {
        List<String> invalidInput = Arrays.asList(labeledInput).stream()
                .filter(triple -> ((Predicate)triple.getRight()).test(triple.getMiddle()))
                .map(triple -> (String)triple.getLeft()).collect(Collectors.toList());

        if (checkWhereClause && mutateQuery.getWhereClause() == null) {
            invalidInput.add("Where Clause");
        }

        if (invalidInput.size() > 0) {
            return ResponseEntity.badRequest().body(Mono.just(new ActionResult(mutationErrorString +
                    String.join(", ", invalidInput))));
        }

        ConnectionInfo info = new ConnectionInfo(mutateQuery.getDatastoreType(), mutateQuery.getDatastoreHost());

        datasetService.notify(new DataNotification(mutateQuery.getDatastoreHost(), mutateQuery.getDatastoreType(),
                mutateQuery.getDatabaseName(), mutateQuery.getTableName(), 1));

        return ResponseEntity.ok().body(queryService.mutateData(info, mutateQuery));
    }
}
