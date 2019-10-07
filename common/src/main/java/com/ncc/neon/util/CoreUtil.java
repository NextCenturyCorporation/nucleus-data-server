package com.ncc.neon.util;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoreUtil {

    private static final String Dot = Pattern.quote(".");
    
    public static Object deepFind(Map<String, Object> map, String pathString) {

        if (pathString.trim().isEmpty() || map == null )
        {
            log.debug("blank pathString, null or non Map item object");
            return null;
        }

        String[] pathArray = pathString.split(Dot);
        Object value = map.get(pathArray[0]);
        if (value instanceof Map)
        {
            if (pathArray.length > 1)
            {
                String nestedFieldNamePath = String.join(".", Arrays.copyOfRange(pathArray, 1, pathArray.length));
                return deepFind((Map<String, Object>)value, nestedFieldNamePath);    
            }
            else
            {
                return value;
            }
        }
        else
        {
            return value;
        }
    }
    
}