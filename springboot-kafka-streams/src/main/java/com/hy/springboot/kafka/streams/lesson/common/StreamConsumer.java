package com.hy.springboot.kafka.streams.lesson.common;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import java.awt.Window;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author hy
 * @date 2022/1/16 12:11 下午
 * @description
 */
public interface StreamConsumer<T>{

    Logger logger = LoggerFactory.getLogger(StreamConsumer.class);

    String getOutputTopicName();

    @KafkaListener(topics = "#{@streamImpl.getOutputTopicName()}")
    default void consumerOutput(ConsumerRecord<?, T> outEvent) {
        if (Objects.isNull(outEvent)) {
            logger.error("consumer is null");
        }
        Object key = outEvent.key();
        T value = outEvent.value();
        logger.info("output {} [key:{} value:{}]", StrUtil.lowerFirst(value.getClass().getSimpleName()), key, value);
    }

}
