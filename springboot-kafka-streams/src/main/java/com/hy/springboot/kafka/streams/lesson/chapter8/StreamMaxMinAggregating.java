package com.hy.springboot.kafka.streams.lesson.chapter8;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.YearMovie;
import com.hy.springboot.kafka.streams.domain.YearlyMovieFigures;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
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
public class StreamMaxMinAggregating extends AbstractStreamProducer<YearMovie> implements StreamConsumer<YearlyMovieFigures> {

    private static final List<YearMovie> TEST_EVENTS = Lists.newArrayList(
            YearMovie.builder()
                    .releaseYear(2019).title("Avengers: Endgame").totalSales(856980506L).build(),
            YearMovie.builder()
                    .releaseYear(2019).title("Captain Marvel").totalSales(426829839L).build(),
            YearMovie.builder()
                    .releaseYear(2019).title("Toy Story 4").totalSales(401486230L).build(),
            YearMovie.builder()
                    .releaseYear(2019).title("The Lion King").totalSales(385082142L).build(),
            YearMovie.builder()
                    .releaseYear(2018).title("Black Panther").totalSales(700059566L).build(),
            YearMovie.builder()
                    .releaseYear(2018).title("Avengers: Infinity War").totalSales(678815482L).build(),
            YearMovie.builder()
                    .releaseYear(2018).title("Deadpool 2").totalSales(324512774L).build(),
            YearMovie.builder()
                    .releaseYear(2017).title("Beauty and the Beast").totalSales(517218368L).build(),
            YearMovie.builder()
                    .releaseYear(2017).title("Wonder Woman").totalSales(412563408L).build(),
            YearMovie.builder()
                    .releaseYear(2017).title("Star Wars Ep. VIII: The Last Jedi").totalSales(517218368L).build());

    @Bean
    public void aggregatingMaxMinStream() {
        Serde<YearlyMovieFigures> domainEventSerde = new JsonSerde<>(YearlyMovieFigures.class, objectMapper);
        Serde<YearMovie> yearMovieSerde = new JsonSerde<>(YearMovie.class, objectMapper);
        //定义名字很关键否则用默认名称存储，再有别的聚合回去反序列化直接报错
        Materialized<Integer, YearlyMovieFigures, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<Integer, YearlyMovieFigures, KeyValueStore<Bytes, byte[]>>as("aggregatingMaxMinStream")
                .withKeySerde(Serdes.Integer())
                .withValueSerde(domainEventSerde);

        streamsBuilder.stream(getInputTopicName(), Consumed.with(Serdes.String(), yearMovieSerde))
                .groupBy(
                        (k, v) -> v.getReleaseYear(),
                        Grouped.with(Serdes.Integer(), yearMovieSerde))
                .aggregate(
                        () -> YearlyMovieFigures.builder()
                                .releaseYear(0).maxTotalSales(Long.MIN_VALUE).minTotalSales(Long.MAX_VALUE).build(),
                        ((key, value, aggregate) ->
                                YearlyMovieFigures.builder().releaseYear(key)
                                        .minTotalSales(Math.min(value.getTotalSales(), aggregate.getMinTotalSales()))
                                        .maxTotalSales(Math.max(value.getTotalSales(), aggregate.getMaxTotalSales()))
                                        .build()),
                        materialized)
                .toStream()
                .peek((k,v)-> log.info("k:{}v:{}",k,v))
                .to("YearlyMovieFigures", Produced.with(Serdes.Integer(), domainEventSerde));
    }

    @Override
    public List<YearMovie> getData() {
        return TEST_EVENTS;
    }

    @Override
    public String getInputTopicName() {
        return getClassName(YearMovie.class);
    }

    @Override
    public String getOutputTopicName() {
        return "YearlyMovieFigures";
    }
}
