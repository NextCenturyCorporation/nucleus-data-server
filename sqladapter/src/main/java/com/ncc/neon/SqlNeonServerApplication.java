package com.ncc.neon;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/*
This class is completly unesssary but there is a bug in VSCode Spring-boot
dashboard to start a project that has a main in a dependency.
I filed an issue with the project tracking 
https://github.com/Microsoft/vscode-spring-boot-dashboard/issues/63
*/
@SpringBootApplication()
public class SqlNeonServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SqlNeonServerApplication.class, args);
    }
}
