package com.bdreport.socket.server.jdbc;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

@Configuration
@EnableAutoConfiguration
@PropertySource({"classpath:/properties/local/jdbc.properties"})
public class JdbcConfig {
    @Configuration
    @Profile("production")
    @PropertySource("classpath:/properties/production/jdbc.properties")
    static class Production
    { }

    @Configuration
    @Profile("local")
    @PropertySource({"classpath:/properties/local/jdbc.properties"})
    static class Local
    { }
}
