package com.hy.springboot.kafka.streams.lesson.chapter14;

import cn.hutool.core.util.IdUtil;
import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.CountAndSum;
import com.hy.springboot.kafka.streams.domain.MessageEvent;
import com.hy.springboot.kafka.streams.domain.MessageEventArr;
import com.hy.springboot.kafka.streams.lesson.common.StreamWindowFormat;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author hy
 * @date 2022/1/22 5:09 下午
 * @description
 */
@Slf4j
@Configuration("streamImpl")
public class StreamSessionTimeWindow extends AbstractStreamProducer<MessageEventArr> implements StreamWindowFormat {

    @Bean
    public void streamUpper() {
        KStream<String, MessageEventArr> stream = streamsBuilder.stream(getInputTopicName());
        Materialized<String, Long, SessionStore<Bytes, byte[]>> sessionWindow = Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("sessionWindow")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long());
        stream.flatMapValues(MessageEventArr::getName)
                .groupBy((key, word) -> word)
                .windowedBy(SessionWindows.with(Duration.ofSeconds(5L)))
                .count(sessionWindow)
                .toStream()
                .peek((x, y) -> log.info("# nameStatistics # {} value:{}", windowedKeyToString(x), y));
    }

    @Override
    public List<MessageEventArr> getData() {
        return Lists.newArrayList(
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy1")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy1")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "xyz")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "ws")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "ws")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "xyz")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy1")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "xyz")).build()
        );
    }

    @Override
    public String getInputTopicName() {
        return getClassName(MessageEvent.class);
    }
}
