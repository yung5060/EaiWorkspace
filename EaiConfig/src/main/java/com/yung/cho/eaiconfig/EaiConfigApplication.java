package com.yung.cho.eaiconfig;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

@EnableConfigServer
@SpringBootApplication
public class EaiConfigApplication {

    public static void main(String[] args) {
        SpringApplication.run(EaiConfigApplication.class, args);
    }

}
