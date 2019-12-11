package com.ncc.neon.controllers;

import com.ncc.neon.models.DataNotification;
import com.ncc.neon.services.DatasetService;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@CrossOrigin(origins="*")
@RestController
@RequestMapping("dataset")
public class DatasetController {

    private DatasetService datasetService;

    DatasetController(DatasetService datasetService) {
        this.datasetService = datasetService;
    }

    /**
     * Subscribes to data notifications and returns the ServerSentEvent listener object.
     */
    @GetMapping(path = "listen", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<ServerSentEvent<DataNotification>>> listen() {
        return ResponseEntity.ok().header("X-Accel-Buffering", "no").body(datasetService.listen());
    }

    /**
     * Sends the given data notification object to all its listeners and returns the timestamp.
     */
    @PostMapping(path = "notify")
    @ResponseStatus(code = HttpStatus.ACCEPTED)
    public ResponseEntity<Mono<String>> notify(@RequestBody() DataNotification notification) {
        return ResponseEntity.ok().body(datasetService.notify(notification));
    }
}
