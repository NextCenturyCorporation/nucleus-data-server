package com.ncc.neon;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.web.reactive.error.DefaultErrorAttributes;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;

@Component
public class CustomErrorAttributes extends DefaultErrorAttributes {

    public CustomErrorAttributes() {
        super(false);
    }

    @Override
    public Map<String, Object> getErrorAttributes(ServerRequest request, boolean includeStackTrace) {
        Map<String, Object> errorMap = super.getErrorAttributes(request, includeStackTrace);
        String stackTrace = errorMap.get("trace").toString();
        Map<String, String> stackTraceMap = new LinkedHashMap<String, String>();
        String[] linesArray = stackTrace.split("\n");
        for (int i = 0; i < linesArray.length; ++i) {
            stackTraceMap.put("" + i, linesArray[i]);
        }
        errorMap.put("trace", stackTraceMap);
        return errorMap;
    }
}
