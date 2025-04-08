package org.razkevich.quartz.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Controller for web pages
 */
@Controller
public class WebController {

    @GetMapping("/")
    public String index() {
        System.out.println("WebController.index() called");
        // This will resolve to src/main/resources/templates/index.html
        return "index";
    }
    
    @GetMapping("/test")
    @ResponseBody
    public String test() {
        return "Test endpoint is working!";
    }
}