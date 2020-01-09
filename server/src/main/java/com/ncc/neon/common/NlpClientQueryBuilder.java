package com.ncc.neon.common;

import org.springframework.http.HttpHeaders;

public class NlpClientQueryBuilder {

    public static HttpHeaders buildFileOperationQuery(String filename) {
        HttpHeaders param = new HttpHeaders();
        param.add("file", filename);
        return param;
    }

    public static HttpHeaders buildFilePrefixQuery(String basename) {
        HttpHeaders param = new HttpHeaders();
        param.add("file_prefix", basename);
        return param;
    }

    public static HttpHeaders buildTrainingOperationQuery(String trainSrc, String trainTgt, String validSrc, String validTgt) {
        String basename = trainSrc.substring(0, trainSrc.length()-7);
        HttpHeaders params = new HttpHeaders();
        params.add("train_src", trainSrc);
        params.add("train_tgt", trainTgt);
        params.add("valid_src", validSrc);
        params.add("valid_tgt", validTgt);
        params.add("output_basename", basename);
        return params;
    }

}
