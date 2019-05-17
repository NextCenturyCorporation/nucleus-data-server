package com.ncc.neon.server.controllers;

import java.util.LinkedHashMap;
import java.util.Map;

import com.ncc.neon.server.services.StateService;
import com.ncc.neon.server.services.StateService.StateServiceFailureException;
import com.ncc.neon.server.services.StateService.StateServiceMissingFileException;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("stateservice")
public class StateController {

    private StateService stateService;

    StateController(StateService stateService) {
        this.stateService = stateService;
    }

    /**
     * Deletes the state with the given name.
     *
     * @param stateName
     * @return Boolean
     */
    @DeleteMapping(path = "deletestate/{stateName}")
    ResponseEntity<Mono<Boolean>> deleteState(@PathVariable String stateName) {
        try {
            stateService.deleteState(stateName);
        }
        catch(StateServiceFailureException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Mono.just(false));
        }
        catch(StateServiceMissingFileException e) {
            return ResponseEntity.ok().body(Mono.just(false));
        }
        System.out.println("State deleted successfully!");
        return ResponseEntity.ok().body(Mono.just(true));
    }

    /**
     * Returns the array of saved state names.
     *
     * @return Array
     */
    @GetMapping(path = "liststates")
    ResponseEntity<Mono<Map[]>> listStates(
        @RequestParam(value = "limit", defaultValue = "10") int limit, 
        @RequestParam(value = "offset", defaultValue = "0") int offset
    ) {
        Map[] states = stateService.listStates(limit, offset);
        return ResponseEntity.ok().body(Mono.just(states));
    }

    /**
     * Returns the data in the state with the given name, or an empty map if no state exists.
     *
     * @param stateName
     * @return Map
     */
    @GetMapping(path = "loadstate")
    ResponseEntity<Mono<Map>> loadState(@RequestParam(value = "stateName") String stateName) {
        try {
            Map stateData = stateService.loadState(stateName);
            return ResponseEntity.ok().body(Mono.just(stateData));
        }
        catch(StateServiceFailureException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Mono.just(null));
        }
        catch(StateServiceMissingFileException e) {
            return ResponseEntity.ok().body(Mono.just(new LinkedHashMap<String, String>()));
        }
    }

    /**
     * Saves the given data as a state with the given name.
     *
     * @param stateName
     * @param stateData
     * @return Boolean
     */
    @PostMapping(path = "savestate", consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    ResponseEntity<Mono<Boolean>> saveState(@RequestParam(value = "stateName") String stateName, @RequestBody Map stateData) {
        try {
            stateService.saveState(stateName, stateData);
        }
        catch(StateServiceFailureException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Mono.just(false));
        }
        return ResponseEntity.ok().body(Mono.just(true));
    }
}
