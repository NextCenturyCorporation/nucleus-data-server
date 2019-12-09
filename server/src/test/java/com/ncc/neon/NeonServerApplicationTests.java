package com.ncc.neon;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = NeonServerApplication.class)
@SpringBootTest
public class NeonServerApplicationTests {
    @Test
    public void contextLoads() {
    }
}
