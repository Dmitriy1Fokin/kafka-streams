package ru.fds.kafka.stream.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.fds.kafka.stream.Constants;
import ru.fds.kafka.stream.dto.Message;
import ru.fds.kafka.stream.dto.TypeEnum;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    private final Constants constants;

    public KafkaStreamsConfig(Constants constants) {
        this.constants = constants;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, constants.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, constants.getBootstrapAddress());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public Serde<Message> messageSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Message.class));
    }

    @Bean
    public KStream<String, Message> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder
                .stream(constants.getTopicNameObject(), Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, Message> messageKStream = stream
                .mapValues(this::getMessageFromString)
                .filter((key, value) -> value.getTypeEnum() == TypeEnum.TYPE_1);
        messageKStream.to(constants.getTopicNameStreamTypeOne(), Produced.with(Serdes.String(), messageSerde()));

        KTable<String, Long> countOfTypeTable = stream
                .mapValues(value -> getMessageFromString(value).getTypeEnum().name())
                .groupBy((key, value) -> value)
                .count();
        countOfTypeTable.toStream().print(Printed.<String, Long>toSysOut().withLabel("Table \"count of type\""));
        countOfTypeTable.toStream().to(constants.getTopicNameStreamTable(), Produced.with(Serdes.String(), Serdes.Long()));

        return messageKStream;
    }

    private Message getMessageFromString(String messageString) {
        Message message = null;
        try {
            message = objectMapper().readValue(messageString, Message.class);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        }
        return message;
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

}
