package com.ncc.neon.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Docfile {
    private String id;
    private String uuid;
    @JsonProperty("text")
    private String text;
    private String first_data;
    private String first_stamp;

    private String domain;
    @JsonProperty("guess-publish-date")
    private String guess_publish_date;
    private int nwords;
    @JsonProperty("crawl-date")
    private String crawl_date;
    private int nchars;
    private String language;
    @JsonProperty("warc-file")
    private String warc_file;
    @JsonProperty("langdetect-confidence")
    private String langdetect_confidence;


    @JsonProperty("PERSON")
    private List<List<String>> PERSON;
    @JsonProperty("ORG")
    private List<List<String>> ORG;
    @JsonProperty("GPE")
    private List<List<String>> GPE;
    @JsonProperty("FAC")
    private List<List<String>> FAC;
    @JsonProperty("LOC")
    private List<List<String>>  LOC;
    @JsonProperty("DATE")
    private List<List<String>>  DATE;
    @JsonProperty("WORK_OF_ART")
    private List<List<String>>  WORK_OF_ART;
    @JsonProperty("TIME")
    private List<List<String>>  TIME;
    @JsonProperty("CARDINAL")
    private List<List<String>>  CARDINAL;
    @JsonProperty("ORDINAL")
    private List<List<String>>  ORDINAL;
    @JsonProperty("MOONEY")
    private List<List<String>>  MOONEY;
    @JsonProperty("MONEY")
    private List<List<String>>  MONEY;
    @JsonProperty("LANGUAGE")
    private List<List<String>>  LANGUAGE;
    @JsonProperty("NORP")
    private List<List<String>>  NORP;
    @JsonProperty("QUANTITY")
    private List<List<String>>  QUANTITY;
    @JsonProperty("EVENT")
    private List<List<String>>  EVENT;
    @JsonProperty("PRODUCT")
    private List<List<String>>  PRODUCT;
    @JsonProperty("PERCENT")
    private List<List<String>>  PERCENT;
    @JsonProperty("LAW")
    private List<List<String>>  LAW;
}
