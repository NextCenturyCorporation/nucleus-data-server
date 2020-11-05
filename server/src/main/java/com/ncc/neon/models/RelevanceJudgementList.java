package com.ncc.neon.models;

import lombok.Data;

import java.io.IOException;
import java.util.ArrayList;

@Data
public class RelevanceJudgementList {
    private String searchTerm;
    public ArrayList<RelevanceJudgement> relList;

    public RelevanceJudgementList() throws IOException {
        this.relList = new ArrayList<RelevanceJudgement>();
        this.searchTerm = "";
    }
}
