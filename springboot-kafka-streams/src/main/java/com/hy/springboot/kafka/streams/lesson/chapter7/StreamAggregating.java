package com.hy.springboot.kafka.streams.lesson.chapter7;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.FromMovie;
import com.hy.springboot.kafka.streams.domain.StatisticDO;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * @author hy
 * @date 2022/1/10 10:00 下午
 * @description
 */
@Slf4j
@Configuration("streamImpl")
public class StreamAggregating extends AbstractStreamProducer<FromMovie> implements StreamConsumer<StatisticDO> {

    @Bean
    public KTable<String, StatisticDO> aggregatingStream() {
        Serde<StatisticDO> domainEventSerde = new JsonSerde<>(StatisticDO.class, objectMapper);

        KStream<String, FromMovie> stream = streamsBuilder.stream(getInputTopicName());
        KTable<String, StatisticDO> reduce = stream.
                filter((k, v) -> Objects.nonNull(k) && Objects.nonNull(v))
                .map((k, v) -> new KeyValue<>(v.getTitle(), new StatisticDO(v.getTitle(), v.getId())))
                .groupByKey(Grouped.with(Serdes.String(), domainEventSerde))
                .reduce(StatisticDO::reduce);
        reduce.toStream().to("aggregatingTopicBean", Produced.with(Serdes.String(), domainEventSerde));
        return reduce;
    }

    //这里比较蛋疼，序列化用了自带的json后Integer边byte[]后只有三个字节无法反序列化成为Integer只能自己转换比较傻逼，用了json统计最好全用类
    void aggregatingConvert() {
        KStream<String, FromMovie> stream = streamsBuilder.stream(getInputTopicName());
        stream.map((k, v) -> {
                    log.info("k:{},v:{}", k, v);
                    return new KeyValue<>(v.getTitle(), v.getId().toString());
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce((value1, value2) -> String
                        .valueOf(Integer.parseInt(value1.replace("\"", "")) + Integer.parseInt(value2.replace("\"", ""))))
                .toStream()
                .map((k, v) -> {
                    log.info("k:{},v:{}", k, v);
                    Map<String, Integer> value = new HashMap<>();
                    value.put(k, Integer.valueOf(v));
                    return new KeyValue<>(k, value);
                })
                .to("aggregatingTopic");
    }

    @Override
    public String getOutputTopicName() {
        return "aggregatingTopicBean";
    }

    @Override
    public String getInputTopicName() {
        return getClassName(FromMovie.class);
    }

    @Override
    public List<FromMovie> getData() {
        return Lists.newArrayList(
                FromMovie.builder()
                        .id(294).title("Die Hard::1988").genre("action").build(),
                FromMovie.builder()
                        .id(294).title("Die Hard::1988").genre("action").build(),
                FromMovie.builder()
                        .id(294).title("Die Hard::1988").genre("action").build(),
                FromMovie.builder()
                        .id(294).title("Die Hard::1988").genre("action").build());
    }
}
