package com.hy.springboot.kafka.streams.lesson.chapter13;

import cn.hutool.core.util.IdUtil;
import com.google.common.collect.Lists;
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
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author hy
 * @date 2022/1/22 5:09 下午
 * @description
 */
@Slf4j
@Configuration("streamImpl")
public class StreamTimeWindow extends AbstractStreamProducer<MessageEventArr> implements StreamWindowFormat {

    @Bean
    public void streamUpper() {
        Materialized<String, Long, WindowStore<Bytes, byte[]>> stringLongWindowStoreMaterialized = Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-temp-stream-store")
                .withValueSerde(Serdes.Long()).withKeySerde(Serdes.String());

        KStream<String, MessageEventArr> stream = streamsBuilder.stream(getInputTopicName());
        stream.flatMapValues(MessageEventArr::getName)
                .groupBy((key, word) -> word)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5L)).grace(Duration.ofMillis(0)))
                .count(stringLongWindowStoreMaterialized)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
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
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "xyz")).build()
        );
    }

    @Override
    public String getInputTopicName() {
        return getClassName(MessageEvent.class);
    }
}
