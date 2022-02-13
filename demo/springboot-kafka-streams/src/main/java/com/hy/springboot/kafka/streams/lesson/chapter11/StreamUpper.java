package com.hy.springboot.kafka.streams.lesson.chapter11;

import cn.hutool.core.util.IdUtil;
import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.MessageEvent;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author hy
 * @date 2022/1/21 5:32 下午
 * @description
 */
@Slf4j
@Configuration("streamImpl")
public class StreamUpper extends AbstractStreamProducer<MessageEvent> {

    @Bean
    public void streamUpper() {
        KStream<String, MessageEvent> stream = streamsBuilder.stream(getInputTopicName());
        stream.map((key, value) -> {
            value.setName(value.getName().toUpperCase());
            return new KeyValue<>(key, value);
        })
                .peek((key, value) -> log.info("# KStream #" + key + " : " + value));
    }

    @Override
    public String getInputTopicName() {
        String className = getClassName(MessageEvent.class);
        log.info("className:{}", className);
        return className;
    }

    @Override
    public List<MessageEvent> getData() {
        return Lists.newArrayList(
                MessageEvent.builder().id(IdUtil.fastSimpleUUID()).name("hy").build(),
                MessageEvent.builder().id(IdUtil.fastSimpleUUID()).name("hy1").build());
    }
}
