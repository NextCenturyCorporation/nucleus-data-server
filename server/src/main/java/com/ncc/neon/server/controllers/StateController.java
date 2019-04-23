package com.ncc.neon.server.controllers;

import java.util.Map;

import com.ncc.neon.server.services.StateService;

import org.springframework.http.MediaType;
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
     * @return Boolean of whether the action was successful.
     */
    @GetMapping(path = "deletestate/{stateName}", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Boolean> deleteState(@PathVariable String stateName) {
        boolean successful = stateService.deleteState(stateName);
        return Mono.just(successful);
    }

    /**
     * Returns the array of saved state names.
     *
     * @return Array
     */
    @GetMapping(path = "allstatesnames", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<String[]> findStateNames() {
        String[] stateNames = stateService.findStateNames();
        return Mono.just(stateNames);
    }

    /**
     * Returns the data in the state with the given name, or an empty map if no state exists.
     *
     * @param stateName
     * @return Map
     */
    @GetMapping(path = "loadstate", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Map> loadState(@RequestParam(value = "stateName") String stateName) {
        Map stateData = stateService.loadState(stateName);
        return Mono.just(stateData);
    }

    /**
     * Saves the given data as a state with the given name.
     *
     * @param stateName
     * @param stateData
     * @return Boolean of whether the action was successful.
     */
    @PostMapping(path = "savestate", produces = MediaType.APPLICATION_JSON_UTF8_VALUE, consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    Mono<Boolean> saveState(@RequestParam(value = "stateName") String stateName, @RequestBody Map stateData) {
        boolean successful = stateService.saveState(stateName, stateData);
        return Mono.just(successful);
    }
}
