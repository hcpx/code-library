package com.hy.flink.streaming.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author hy
 * @date 2022/2/7 5:54 下午
 * @description
 */
@Slf4j
public class KafkaStream {

    private static final String TOPIC = "flinkTopic";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "test");
//        生产消费者同时使用这个才可以
//        TypeInformationSerializationSchema<JsonEvent> serializationSchema =
//                new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<JsonEvent>() {
//                }), env.getConfig());
        final FlinkKafkaConsumer<JsonEvent> mySource = new FlinkKafkaConsumer<>(TOPIC, new JsonDeserializationSchema(), properties);
        mySource.setStartFromLatest();
//        mySource.setStartFromTimestamp();
        DataStream<JsonEvent> stream = env.addSource(mySource);
        stream.print();
//        CompletableFuture.runAsync(KafkaStream::kafkaProducerWithJson);
        CompletableFuture.runAsync(KafkaStream::kafkaProducerWithString);
        env.execute("KafkaConsumer");
    }

    @SneakyThrows
    private static void kafkaProducerWithJson() {
        log.info("kafkaProducerWithJson start");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<JsonEvent> stream = env
                .fromElements(JsonEvent.builder().id(1).name("hy").build(), JsonEvent.builder().id(2).name("wy").build());
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");

//        生产消费者同时使用这个才可以
        TypeInformationSerializationSchema<JsonEvent> serializationSchema =
                new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<JsonEvent>() {
                }), env.getConfig());
        FlinkKafkaProducer<JsonEvent> myProducer = new FlinkKafkaProducer<>(
                TOPIC,                  // 目标 topic
                serializationSchema,     // 序列化 schema
                properties); // 配置
        Thread.sleep(10000);
        stream.addSink(myProducer);
        env.execute("kafkaProducerWithJson");
    }

    @SneakyThrows
    private static void kafkaProducerWithString() {
        log.info("kafkaProducerWithString start");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env
                .fromElements(objectMapper.writeValueAsString(JsonEvent.builder().id(1).name("hy").build()), objectMapper
                        .writeValueAsString(JsonEvent.builder().id(2).name("wy").build()));
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                TOPIC,                  // 目标 topic
                new SimpleStringSchema(),     // 序列化 schema
                properties); // 配置
        Thread.sleep(10000);
        stream.addSink(myProducer);
        env.execute("kafkaProducerWithString");
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    static class JsonEvent {

        private Integer id;
        private String name;
    }

    static class JsonDeserializationSchema implements DeserializationSchema<JsonEvent> {

        private static final long serialVersionUID = 1L;

        private static final ObjectMapper objectMapper = new ObjectMapper();


        @Override
        public JsonEvent deserialize(byte[] message) throws IOException {
            return objectMapper.readValue(message, JsonEvent.class);
        }

        @Override
        public boolean isEndOfStream(JsonEvent nextElement) {
            return false;
        }

        @Override
        public TypeInformation<JsonEvent> getProducedType() {
            return TypeInformation.of(JsonEvent.class);
        }
    }

    static class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, JsonEvent>> {

        private static final String encoding = "UTF8";

        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public boolean isEndOfStream(ConsumerRecord<String, JsonEvent> nextElement) {
            return false;
        }

        @Override
        public ConsumerRecord<String, JsonEvent> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
       /* System.out.println("Record--partition::"+record.partition());
        System.out.println("Record--offset::"+record.offset());
        System.out.println("Record--timestamp::"+record.timestamp());
        System.out.println("Record--timestampType::"+record.timestampType());
        System.out.println("Record--checksum::"+record.checksum());
        System.out.println("Record--key::"+record.key());
        System.out.println("Record--value::"+record.value());*/
            return new ConsumerRecord<>(record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType(),
                    record.checksum(),
                    record.serializedKeySize(),
                    record.serializedValueSize(),
                    /*这里我没有进行空值判断，生产一定记得处理*/
                    new String(record.key(), encoding),
                    objectMapper.readValue(new String(record.value(), encoding), JsonEvent.class));
        }

        @Override
        public TypeInformation<ConsumerRecord<String, JsonEvent>> getProducedType() {
            return TypeInformation.of(new TypeHint<ConsumerRecord<String, JsonEvent>>() {
            });
        }
    }
}
