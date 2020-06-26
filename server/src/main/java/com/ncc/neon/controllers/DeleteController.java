package com.ncc.neon.controllers;

import com.ncc.neon.models.ConnectionInfo;
import com.ncc.neon.models.DataNotification;
import com.ncc.neon.models.queries.MutateQuery;
import com.ncc.neon.models.results.ActionResult;
import com.ncc.neon.services.DatasetService;
import com.ncc.neon.services.QueryService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("deleteservice")
public class DeleteController {
    private DatasetService datasetService;
    private QueryService queryService;

    DeleteController(DatasetService datasetService, QueryService queryService) {
        this.datasetService = datasetService;
        this.queryService = queryService;
    }

    @PostMapping(path="/byid", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Mono<ActionResult>> deleteDataById(@RequestBody MutateQuery mutateQuery) {

        final Predicate<String> isBlank = StringUtils::isBlank;

        Triple[] labeledInput = new Triple[]{
                Triple.of("Datastore Host", mutateQuery.getDatastoreHost(), isBlank),
                Triple.of("Datastore Type", mutateQuery.getDatastoreType(), isBlank),
                Triple.of("Database Name", mutateQuery.getDatabaseName(), isBlank),
                Triple.of("Table Name", mutateQuery.getTableName(), isBlank),
                Triple.of("ID Field", mutateQuery.getIdFieldName(), isBlank),
                Triple.of("Data ID", mutateQuery.getDataId(), isBlank)
        };

        List<String> invalidInput = Arrays.asList(labeledInput).stream()
                .filter(triple -> ((Predicate)triple.getRight()).test(triple.getMiddle()))
                .map(triple -> (String)triple.getLeft()).collect(Collectors.toList());

        if (invalidInput.size() > 0) {
            return ResponseEntity.badRequest().body(Mono.just(new ActionResult("Mutation by ID Query Missing " +
                    String.join(", ", invalidInput))));
        }

        ConnectionInfo info = new ConnectionInfo(mutateQuery.getDatastoreType(), mutateQuery.getDatastoreHost());

        datasetService.notify(new DataNotification(mutateQuery.getDatastoreHost(), mutateQuery.getDatastoreType(),
                mutateQuery.getDatabaseName(), mutateQuery.getTableName(), 1));

        return ResponseEntity.ok().body(queryService.deleteData(info, mutateQuery));
    }

    @PostMapping(path="/byfilter", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE, produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public ResponseEntity<Mono<ActionResult>> deleteDataByFilter(@RequestBody MutateQuery mutateQuery) {

        final Predicate<String> isBlank = StringUtils::isBlank;

        Triple[] labeledInput = new Triple[]{
                Triple.of("Datastore Host", mutateQuery.getDatastoreHost(), isBlank),
                Triple.of("Datastore Type", mutateQuery.getDatastoreType(), isBlank),
                Triple.of("Database Name", mutateQuery.getDatabaseName(), isBlank),
                Triple.of("Table Name", mutateQuery.getTableName(), isBlank),
                Triple.of("ID Field", mutateQuery.getIdFieldName(), isBlank)
        };

        List<String> invalidInput = Arrays.asList(labeledInput).stream()
                .filter(triple -> ((Predicate)triple.getRight()).test(triple.getMiddle()))
                .map(triple -> (String)triple.getLeft()).collect(Collectors.toList());

        if (mutateQuery.getWhereClause() == null) {
            invalidInput.add("Where Clause");
        }

        if (invalidInput.size() > 0) {
            return ResponseEntity.badRequest().body(Mono.just(new ActionResult("Deletion by filter Query Missing " +
                    String.join(", ", invalidInput))));
        }

        ConnectionInfo info = new ConnectionInfo(mutateQuery.getDatastoreType(), mutateQuery.getDatastoreHost());

        datasetService.notify(new DataNotification(mutateQuery.getDatastoreHost(), mutateQuery.getDatastoreType(),
                mutateQuery.getDatabaseName(), mutateQuery.getTableName(), 1));

        return ResponseEntity.ok().body(queryService.deleteData(info, mutateQuery));
    }
}
