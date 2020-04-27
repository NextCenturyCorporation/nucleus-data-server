package com.ncc.neon.services;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ClusterServiceTest.class)
public class ClusterServiceTest {

    @Test
    public void placeholderTest() {
        assertTrue(Boolean.TRUE);
    }
}
