package com.hy.springboot.kafka.streams.lesson.chapter9;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.Rating;
import com.hy.springboot.kafka.streams.lesson.common.StreamWindowFormat;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
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
public class StreamTimeWindow extends AbstractStreamProducer<Rating> implements StreamWindowFormat {

    private static final List<Rating> TEST_EVENTS = Lists.newArrayList(
            Rating.builder().title("Die Hard").releaseYear(1998).rating(8.2)
                    .timestamp("2019-04-25T18:00:00-0700").build(),
            Rating.builder().title("Die Hard").releaseYear(1998).rating(4.5)
                    .timestamp("2019-04-25T18:03:00-0700").build(),
            Rating.builder().title("Die Hard").releaseYear(1998).rating(5.1)
                    .timestamp("2019-04-25T18:04:00-0700").build(),
            Rating.builder().title("Die Hard").releaseYear(1998).rating(2.0)
                    .timestamp("2019-04-25T18:07:00-0700").build(),
            Rating.builder().title("Die Hard").releaseYear(1998).rating(8.3)
                    .timestamp("2019-04-25T18:32:00-0700").build(),
            Rating.builder().title("Die Hard").releaseYear(1998).rating(3.4)
                    .timestamp("2019-04-25T18:36:00-0700").build(),
            Rating.builder().title("Die Hard").releaseYear(1998).rating(4.2)
                    .timestamp("2019-04-25T18:43:00-0700").build(),
            Rating.builder().title("Die Hard").releaseYear(1998).rating(7.6)
                    .timestamp("2019-04-25T18:44:00-0700").build(),

            Rating.builder().title("Tree of Life").releaseYear(2011).rating(4.9)
                    .timestamp("2019-04-25T20:01:00-0700").build(),
            Rating.builder().title("Tree of Life").releaseYear(2011).rating(5.6)
                    .timestamp("2019-04-25T20:02:00-0700").build(),
            Rating.builder().title("Tree of Life").releaseYear(2011).rating(9.0)
                    .timestamp("2019-04-25T20:03:00-0700").build(),
            Rating.builder().title("Tree of Life").releaseYear(2011).rating(6.5)
                    .timestamp("2019-04-25T20:12:00-0700").build(),
            Rating.builder().title("Tree of Life").releaseYear(2011).rating(2.1)
                    .timestamp("2019-04-25T20:13:00-0700").build(),

            Rating.builder().title("A Walk in the Clouds").releaseYear(1995).rating(3.6)
                    .timestamp("2019-04-25T22:20:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1995).rating(6.0)
                    .timestamp("2019-04-25T22:21:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1995).rating(7.0)
                    .timestamp("2019-04-25T22:22:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1995).rating(4.6)
                    .timestamp("2019-04-25T22:23:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1995).rating(7.1)
                    .timestamp("2019-04-25T22:24:00-0700").build(),

            Rating.builder().title("A Walk in the Clouds").releaseYear(1998).rating(9.9)
                    .timestamp("2019-04-25T21:15:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1998).rating(8.9)
                    .timestamp("2019-04-25T21:16:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1998).rating(7.9)
                    .timestamp("2019-04-25T21:17:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1998).rating(8.9)
                    .timestamp("2019-04-25T21:18:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1998).rating(9.9)
                    .timestamp("2019-04-25T21:19:00-0700").build(),
            Rating.builder().title("A Walk in the Clouds").releaseYear(1998).rating(9.9)
                    .timestamp("2019-04-25T21:20:00-0700").build(),

            Rating.builder().title("Super Mario Bros.").releaseYear(1993).rating(3.5)
                    .timestamp("2019-04-25T13:00:00-0700").build(),
            Rating.builder().title("Super Mario Bros.").releaseYear(1993).rating(4.5)
                    .timestamp("2019-04-25T13:07:00-0700").build(),
            Rating.builder().title("Super Mario Bros.").releaseYear(1993).rating(5.5)
                    .timestamp("2019-04-25T13:30:00-0700").build(),
            Rating.builder().title("Super Mario Bros.").releaseYear(1993).rating(6.5)
                    .timestamp("2019-04-25T13:34:00-0700").build());

    @Bean
    public void timeWindowStream() {
        Serde<Rating> ratingSerde = new JsonSerde<>(Rating.class, objectMapper);

        streamsBuilder.stream(getInputTopicName(), Consumed.with(Serdes.String(), ratingSerde))
                .map((key, rating) -> new KeyValue<>(rating.getTitle(), rating))
                .groupByKey(Grouped.with(Serdes.String(), ratingSerde))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)))
                .count(Materialized.as("timeWindowStream"))
                .toStream()
                .map((Windowed<String> key, Long count) -> new KeyValue<>(windowedKeyToString(key), count.toString()))
                .peek((k, v) -> log.info("k:{} v:{}", k, v));
    }

    @Override
    public List<Rating> getData() {
        return TEST_EVENTS;
    }

    @Override
    public String getInputTopicName() {
        return getClassName(Rating.class);
    }
}
