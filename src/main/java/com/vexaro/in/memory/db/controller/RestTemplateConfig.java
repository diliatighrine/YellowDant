package com.vexaro.in.memory.db.controller;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate() {
        // Instancier RestTemplate sans spécifier de factory
        return new RestTemplate();
    }
}
