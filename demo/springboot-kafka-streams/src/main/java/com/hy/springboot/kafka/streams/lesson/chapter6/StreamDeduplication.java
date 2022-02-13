package com.hy.springboot.kafka.streams.lesson.chapter6;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.FromMovie;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author HY
 * @version 0.0.1
 * @date 2021/2/1 11:01
 */
@Slf4j
@Configuration("streamImpl")
public class StreamDeduplication extends AbstractStreamProducer<FromMovie> implements StreamConsumer<FromMovie> {

    @Bean
    public void deduplicationStream() {
        Duration windowSize = Duration.ofSeconds(3);

        StoreBuilder<WindowStore<String, Long>> deduplicationStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(DeduplicationTransformer.storeName,
                        windowSize,
                        windowSize,
                        false
                ),
                Serdes.String(),
                Serdes.Long());

        streamsBuilder.addStateStore(deduplicationStoreBuilder);
        KStream<String, FromMovie> fromStream = streamsBuilder.stream("fromMovie");
        fromStream.transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(), (key, value) -> value
                .getTitle()), DeduplicationTransformer.storeName)
                .to("after-deduplication");
    }

    @Override
    public String getInputTopicName() {
        return getClassName(FromMovie.class);
    }

    @Override
    public List<FromMovie> getData() {
        return Lists.newArrayList(
                FromMovie.builder()
                        .genre("George R. R. Martin").title("A T Song of Ice and Fire").build(),
                FromMovie.builder()
                        .genre("George R. R. Martin").title("A T Song of Ice and Fire").build(),
                FromMovie.builder()
                        .genre("C.S. Lewis").title("The Silver Chair").build(),
                FromMovie.builder()
                        .genre("C.S. Lewis").title("Perelandra T").build(),
                FromMovie.builder()
                        .genre("George R. R. Martin").title("Fire & Blood").build());
    }

    @Override
    public String getOutputTopicName() {
        return "after-deduplication";
    }
}
