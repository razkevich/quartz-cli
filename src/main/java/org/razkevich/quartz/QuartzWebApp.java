package org.razkevich.quartz;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Spring Boot application for Quartz Web Console
 */
@SpringBootApplication
@ComponentScan(basePackages = {"org.razkevich.quartz", "org.razkevich.quartz.web"})
public class QuartzWebApp {
    
    public static void main(String[] args) {
        SpringApplication.run(QuartzWebApp.class, args);
    }
}