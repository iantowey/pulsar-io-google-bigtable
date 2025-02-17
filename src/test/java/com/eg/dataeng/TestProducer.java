package com.eg.dataeng;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.demo.dataeng.BigTableSink;
import com.demo.dataeng.BigTableSinkMessage;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.instancio.Instancio;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.instancio.Select.field;

public class TestProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static PulsarClient client = null;
    private static final String SERVICE_URL = "pulsar://localhost:6650";

    static {
        try {
            client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
    public static <T> Producer<T> createProducer(String topicName, Schema<T> schema, String name) {
        try {
            return client.newProducer(schema)
                    .producerName(name)
                    .topic(topicName)
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
    public static BiFunction<String, String, Producer<BigTableSinkMessage>> PRODUCER = (topicName, producerName) -> createProducer(topicName, Schema.JSON(BigTableSinkMessage.class), producerName);

    public static void main(String[] args) throws IOException {

        Producer<BigTableSinkMessage> producer = PRODUCER.apply("persistent://public/default/test-bigtable-sink", "bigtable producer11");
        Stream.iterate(0, i -> i + 1).forEach(i -> {
            BigTableSinkMessage message = Instancio.of(BigTableSinkMessage.class)
//                    .generate(field(BigTableSinkMessage::getRowKey), gen -> gen.oneOf("k1", "k2", "k3", "k4", "k5", "k6", "k7"))
                    .generate(field(BigTableSinkMessage::getRowKey), gen -> gen.text().pattern("#C#C#C#C#C#C#C#C#C#C#C#C-#d#d-#d#d-#d#d"))
                    .generate(field(BigTableSinkMessage::getRow), gen -> gen.map().withKeys("coupon-offer-cols").maxSize(1).minSize(1))
                    .create();
            try {
                producer.send(message);
//                Thread.sleep(100);
                System.out.println("record # " + i);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }

        });
        producer.close();
        client.close();
    }
}
