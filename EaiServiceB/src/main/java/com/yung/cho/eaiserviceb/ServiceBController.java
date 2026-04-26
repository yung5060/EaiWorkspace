package com.yung.cho.eaiserviceb;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/")
public class ServiceBController {

    @PostMapping("/")
    public ResponseEntity<String> helloWorld() {
        return ResponseEntity.ok("Hello World from Service B");
    }
}
