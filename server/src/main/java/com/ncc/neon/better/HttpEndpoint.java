package com.ncc.neon.better;

import com.ncc.neon.models.EndpointType;
import org.springframework.http.HttpMethod;

/*
Data class to represent an endpoint of an NLP module.
 */
public class HttpEndpoint {
    private String pathSegment;
    private HttpMethod method;
    // The type controls how endpoints are mapped to specific NLP operations.
    private EndpointType type;

    public HttpEndpoint() {}

    public HttpEndpoint(String pathSegment, HttpMethod method, EndpointType type) {
        this.pathSegment = pathSegment;
        this.method = method;
        this.type = type;
    }

    public String getPathSegment() {
        return pathSegment;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public EndpointType getType() {
        return this.type;
    }
}
