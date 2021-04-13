package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class IRDocEntities {
    private String docid;
    private List<String> entities;
    private List<String> relations;
    private int eventCount;
    private int ssCount;
}
