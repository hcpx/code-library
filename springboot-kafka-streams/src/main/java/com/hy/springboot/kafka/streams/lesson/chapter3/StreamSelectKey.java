package com.hy.springboot.kafka.streams.lesson.chapter3;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.SelectKeyInput;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author HY
 * @version 0.0.1
 * @date 2021/2/1 11:01
 */
@Slf4j
@Configuration("streamImpl")
public class StreamSelectKey extends AbstractStreamProducer<SelectKeyInput> {

    @Bean
    public KStream<String, SelectKeyInput> mapStream() {
        KStream<String, SelectKeyInput> toStream = streamsBuilder.<String, SelectKeyInput>stream(getInputTopicName())
                .selectKey((k, v) -> {
                    String[] fields = v.getPhoneNumber().split("\\|");
                    return fields[fields.length - 1].trim().substring(0, 4);
                });
        toStream.peek((k, v) -> log.info("after selectKey:{} {}", k, v));
        return toStream;
    }

    @Override
    public String getInputTopicName() {
        return getClassName(SelectKeyInput.class);
    }

    @Override
    public List<SelectKeyInput> getData() {
        return Lists.newArrayList(
                SelectKeyInput.builder().id(3).firstName("San").lastName("Zhang").phoneNumber("13910010000").build()
        );
    }

}
