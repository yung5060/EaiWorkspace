package com.yung.cho.eaigateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.PropertySource;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class RoutingConfig {

    @Bean(name = "routingMap")
    public Map<String, String> routingMap(ConfigurableEnvironment env) {
        Map<String, String> map = new LinkedHashMap<>();
        for(PropertySource<?> ps : env.getPropertySources()) {
            if (ps instanceof EnumerablePropertySource<?> eps) {
                for (String name : eps.getPropertyNames()) {
                    Object value = eps.getProperty(name);
                    map.putIfAbsent(name, value.toString());
                }
            }
        }
        return map;
    }
}
