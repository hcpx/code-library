package com.hy.springboot.kafka.streams.lesson.chapter5;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.FromMovie;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.CollectionUtils;

/**
 * @author HY
 * @version 0.0.1
 * @date 2021/2/1 11:01
 */
@Slf4j
@Configuration("streamImpl")
public class StreamMerge extends AbstractStreamProducer<FromMovie> implements StreamConsumer<FromMovie> {

    private boolean topicSwitch = true;

    @Bean
    public void mapStream() {
        KStream<String, FromMovie> topicSwitchTrue = streamsBuilder.stream("topicSwitchTrue");
        KStream<String, FromMovie> topicSwitchFalse = streamsBuilder.stream("topicSwitchFalse");
        topicSwitchTrue.merge(topicSwitchFalse).to("topicSwitchMerge");
    }

    @Override
    public String getInputTopicName() {
        return "topicSwitchTrue";
    }

    @KafkaListener(topics = "topicSwitchFalse")
    void consumerTopicSwitchFalse(ConsumerRecord<String, FromMovie> inputEvent) {
        log.info("topicSwitchFalse");
        consumerInput(inputEvent);
    }

    @Override
    @Scheduled(fixedRate = 2000)
    public void send() {
        if (isFirst) {
            data = getData();
            isFirst = false;
        }
        if (CollectionUtils.isEmpty(data)) {
            return;
        }
        FromMovie fromMovie = getData().get(0);
        Optional.ofNullable(fromMovie).ifPresent(one -> {
            if (topicSwitch) {
                kafkaTemplate.send("topicSwitchTrue", String.valueOf(fromMovie.getId()),
                        fromMovie);
            } else {
                kafkaTemplate.send("topicSwitchFalse", String.valueOf(fromMovie.getId()),
                        fromMovie);
            }
            topicSwitch = !topicSwitch;
        });
        data.remove(0);
        if (data.size() == 0) {
            log.info("send end!");
        }
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

    @Override
    public String getOutputTopicName() {
        return "topicSwitchMerge";
    }
}
