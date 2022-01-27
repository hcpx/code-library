package com.hy.springboot.kafka.streams.lesson.chapter1;

import cn.hutool.core.util.IdUtil;
import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.Movie;
import com.hy.springboot.kafka.streams.domain.RawMovie;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
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
public class StreamMap extends AbstractStreamProducer<RawMovie> implements StreamConsumer<Movie> {

    @Override
    public List<RawMovie> getData() {
        return Lists.newArrayList(
                RawMovie.builder().id(294).title("Die Hard::1988").genre("action").build(),
                RawMovie.builder().id(354).title("Tree of Life::2011").genre("drama").build(),
                RawMovie.builder().id(782).title("A Walk in the Clouds::1995").genre("romance").build(),
                RawMovie.builder().id(128).title("The Big Lebowski::1998").genre("comedy").build()
        );
    }

    @Bean
    public KStream<String, Movie> mapStream() {
        KStream<String, Movie> map = streamsBuilder.<String, RawMovie>stream(getInputTopicName())
                .map((key, rawMovie) -> new KeyValue<>(IdUtil.fastSimpleUUID(), parseRawMovie(rawMovie)));
        map.to("movie");
        return map;
    }

    private Movie parseRawMovie(RawMovie rawMovie) {
        String[] titleParts = rawMovie.getTitle().split("::");
        String title = titleParts[0];
        int releaseYear = Integer.parseInt(titleParts[1]);
        return Movie.builder()
                .id(rawMovie.getId())
                .title(title)
                .releaseYear(releaseYear)
                .genre(rawMovie.getGenre())
                .build();
    }

    @Override
    public String getInputTopicName() {
        return getClassName(RawMovie.class);
    }

    @Override
    public String getOutputTopicName() {
        return getClassName(Movie.class);
    }
}
