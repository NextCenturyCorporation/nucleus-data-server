package com.ncc.neon.better;

import org.springframework.http.HttpMethod;

public class HttpEndpoint {
    private String pathSegment;
    private HttpMethod method;

    public HttpEndpoint() {}

    public HttpEndpoint(String pathSegment, HttpMethod method) {
        this.pathSegment = pathSegment;
        this.method = method;
    }

    public String getPathSegment() {
        return pathSegment;
    }

    public HttpMethod getMethod() {
        return method;
    }
}
