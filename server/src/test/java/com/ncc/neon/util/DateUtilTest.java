package com.ncc.neon.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import com.ncc.neon.util.DateUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=DateUtil.class)
public class DateUtilTest {

    @Test
    public void transformDateToStringTest() {
        ZonedDateTime date = ZonedDateTime.of(2019, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        String string = DateUtil.transformDateToString(date);
        assertThat(string).isEqualTo("2019-01-01T00:00:00Z");
    }

    @Test
    public void transformDateToStringWithNonZeroSecondsTest() {
        ZonedDateTime date = ZonedDateTime.of(2019, 1, 1, 0, 0, 30, 500000000, ZoneOffset.UTC);
        String string = DateUtil.transformDateToString(date);
        assertThat(string).isEqualTo("2019-01-01T00:00:30.5Z");
    }

    @Test
    public void transformDateToStringWithNonZeroOffsetTest() {
        ZonedDateTime date = ZonedDateTime.of(2019, 01, 01, 0, 0, 0, 0, ZoneOffset.ofHours(1));
        String string = DateUtil.transformDateToString(date);
        assertThat(string).isEqualTo("2019-01-01T00:00:00+01:00");
    }

    @Test
    public void transformStringToDateWithHoursAndOffsetTest() {
        ZonedDateTime date = DateUtil.transformStringToDate("2019-01-01T00:00Z");
        assertThat(date.toString()).isEqualTo("2019-01-01T00:00Z");
    }

    @Test
    public void transformStringToDateWithHoursAndNonZeroOffsetTest() {
        ZonedDateTime date = DateUtil.transformStringToDate("2019-01-01T00:00+01:00");
        assertThat(date.toString()).isEqualTo("2019-01-01T00:00+01:00");
    }

    @Test
    public void transformStringToDateWithSecondsAndOffsetTest() {
        ZonedDateTime date = DateUtil.transformStringToDate("2019-01-01T00:00:00Z");
        assertThat(date.toString()).isEqualTo("2019-01-01T00:00Z");
    }

    @Test
    public void transformStringToDateWithSecondsAndNonZeroOffsetTest() {
        ZonedDateTime date = DateUtil.transformStringToDate("2019-01-01T00:00:00+01:00");
        assertThat(date.toString()).isEqualTo("2019-01-01T00:00+01:00");
    }

    @Test
    public void transformStringToDateWithNonZeroSecondsAndOffsetTest() {
        ZonedDateTime date = DateUtil.transformStringToDate("2019-01-01T00:00:30.500Z");
        assertThat(date.toString()).isEqualTo("2019-01-01T00:00:30.500Z");
    }
}
