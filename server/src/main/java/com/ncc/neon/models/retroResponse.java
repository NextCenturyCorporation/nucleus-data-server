package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class retroResponse {
    @JsonProperty("uuids")
    private String[] uuids;

}
