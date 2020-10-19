package ru.fds.kafka.stream;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "ru.fds.kafka.streams")
public class Constants {

    private String bootstrapAddress;
    private String applicationId;
    private String topicNameObject;
    private String topicNameStreamTypeOne;
}
