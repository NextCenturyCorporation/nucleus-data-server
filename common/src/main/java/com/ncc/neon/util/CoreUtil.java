package com.ncc.neon.util;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoreUtil {

    private static final String DOT = Pattern.quote(".");
    
    /**
     * Queries a given object to retrieve a value nested inside the object using the given path string (with periods to mark each nested property).
     * eg. path string "address.geoLocation.lat" queries the value of "lat" which is nested three levels deep
     * @param map  object to be queried
     * @param pathString a "." separated path string representing the query.
     * @return returns the value retreived
     */
    public static Object deepFind(Map<String, Object> map, String pathString) {

        if (pathString.trim().isEmpty() || map == null )
        {
            log.debug("blank pathString, null or non Map item object");
            return null;
        }

        String[] pathArray = pathString.split(DOT);
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