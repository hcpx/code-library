package com.hy.springboot.kafka.streams.lesson.chapter4;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.FromMovie;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author HY
 * @version 0.0.1
 * @date 2021/2/1 11:01
 */
@Slf4j
@Configuration("streamImpl")
public class StreamBranch extends AbstractStreamProducer<FromMovie> {

    @Bean
    public void mapStream() {
        KStream<String, FromMovie>[] branch = streamsBuilder.<String, FromMovie>stream(getInputTopicName())
                .branch((k, v) -> v.getTitle().contains("T"), (k, v) -> !v.getTitle().contains("T"));
        branch[0].to("containsT");
        branch[1].to("notContainsT");
    }

    @Override
    public String getInputTopicName() {
        return getClassName(FromMovie.class);
    }

    @KafkaListener(topics = "containsT")
    public void consumeOutputContainT(ConsumerRecord<String, FromMovie> inputEvent) {
        log.info("containsT");
        consumerInput(inputEvent);
    }

    @KafkaListener(topics = "notContainsT")
    public void consumeOutputNotContainT(ConsumerRecord<String, FromMovie> inputEvent) {
        log.info("notContainsT");
        consumerInput(inputEvent);
    }

    @Override
    public List<FromMovie> getData() {
        return Lists.newArrayList(
                FromMovie.builder()
                        .genre("George R. R. Martin").title("A T Song of Ice and Fire").build(),
                FromMovie.builder()
                        .genre("C.S. Lewis").title("The Silver Chair").build(),
                FromMovie.builder()
                        .genre("C.S. Lewis").title("Perelandra T").build(),
                FromMovie.builder()
                        .genre("George R. R. Martin").title("Fire & Blood").build()
        );
    }
}
