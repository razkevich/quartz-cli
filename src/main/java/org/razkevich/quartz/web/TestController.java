package org.razkevich.quartz.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Test controller to verify REST endpoints are working
 */
@RestController
@RequestMapping("/api/test")
public class TestController {

    @GetMapping("/ping")
    public Map<String, String> ping() {
        System.out.println("TestController.ping() called");
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Quartz Web Console API is working!");
        return response;
    }
}