package com.hy.responseresult;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springfox.documentation.oas.annotations.EnableOpenApi;

@EnableOpenApi
@SpringBootApplication
public class ResponseResultApplication {

    public static void main(String[] args) {
        SpringApplication.run(ResponseResultApplication.class, args);
    }

}
