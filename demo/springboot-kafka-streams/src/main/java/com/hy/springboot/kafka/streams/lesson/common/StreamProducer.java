package com.hy.springboot.kafka.streams.lesson.common;

import cn.hutool.core.util.StrUtil;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author hy
 * @date 2022/1/16 10:55 上午
 * @description
 */
public interface StreamProducer<T> {

    Logger logger = LoggerFactory.getLogger(StreamProducer.class);

    List<T> getData();

    void send();

    String getInputTopicName();

    @KafkaListener(topics = "#{@streamImpl.getInputTopicName()}")
    default void consumerInput(ConsumerRecord<String, T> inputEvent) {
        if (Objects.isNull(inputEvent)) {
            logger.error("consumer is null");
        }
        String key = inputEvent.key();
        T value = inputEvent.value();
        logger.info("input {} [key:{} value:{}]", StrUtil.lowerFirst(value.getClass().getSimpleName()), key, value);
    }

    default String getClassName(Class<?> clz) {
        return StrUtil.lowerFirst(clz.getSimpleName());
    }
}
