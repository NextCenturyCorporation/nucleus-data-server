package com.ncc.neon.util;

import java.time.DateTimeException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtil {
    public static ZonedDateTime transformStringToDate(String string) {
        // TODO Add non-ISO date parsers
        try {
            return ZonedDateTime.from(DateTimeFormatter.ISO_DATE_TIME.parse(string));
        }
        catch(DateTimeException e1) {
            return null;
        }
    }

    public static String transformDateToString(ZonedDateTime date) {
        // TODO Add non-ISO date formatters
        try {
            return DateTimeFormatter.ISO_DATE_TIME.format(date);
        }
        catch(DateTimeException e1) {
            return null;
        }
    }
}