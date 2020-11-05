package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class IRResponse {
    @JsonProperty("uuids")
    private List<String> uuids;

}
