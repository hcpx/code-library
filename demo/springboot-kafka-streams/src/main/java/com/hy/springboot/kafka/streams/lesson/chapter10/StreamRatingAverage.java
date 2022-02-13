package com.hy.springboot.kafka.streams.lesson.chapter10;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.CountAndSum;
import com.hy.springboot.kafka.streams.domain.RatingChapter10;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
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
public class StreamRatingAverage extends AbstractStreamProducer<RatingChapter10> implements StreamConsumer<CountAndSum> {

    private static final List<RatingChapter10> TEST_EVENTS = Lists.newArrayList(
            RatingChapter10.builder().movieId(362).rating(9.6).build(),
            RatingChapter10.builder().movieId(362).rating(9.7).build(),
            RatingChapter10.builder().movieId(362).rating(8.6).build());

    @Bean
    public void ratingAverageStream() {
        Serde<RatingChapter10> ratingSerde = new JsonSerde<>(RatingChapter10.class, objectMapper);
        Serde<CountAndSum> countAndSumSerde = new JsonSerde<>(CountAndSum.class, objectMapper);

        Grouped<Integer, Double> with = Grouped.with(Serdes.Integer(), Serdes.Double());

        streamsBuilder
                .stream(getInputTopicName(), Consumed.with(Serdes.String(), ratingSerde))
                .map((key, rating) -> new KeyValue<>(rating.getMovieId(), rating.getRating()))
                .groupByKey(with)
                .aggregate(() -> CountAndSum.builder().count(0).sum(0D).build(),
                        (k, v, countAndSum) -> CountAndSum.builder().count(countAndSum.getCount() + 1).sum(countAndSum.getSum() + v)
                                .build(),
                        Materialized.<Integer, CountAndSum, KeyValueStore<Bytes, byte[]>>as("ratingAverage1")
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(countAndSumSerde)
                )
                .mapValues((v) -> {
                    log.info("value:{}", v);
                    return (v.getSum() / v.getCount());
                })
                .toStream()
                .to("afterRatingAverage");
    }

    @Override
    public List<RatingChapter10> getData() {
        return TEST_EVENTS;
    }

    @Override
    public String getInputTopicName() {
        return getClassName(RatingChapter10.class);
    }

    @Override
    public String getOutputTopicName() {
        return "afterRatingAverage";
    }
}
