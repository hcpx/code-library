package com.hy.springboot.kafka.streams.lesson.chapter15;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.TempHumidity;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.StreamWindowFormat;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * @author hy
 * @date 2022/1/22 5:09 下午
 * @description
 */
@Slf4j
@Configuration("streamImpl")
public class StreamFilterTimeWindow extends AbstractStreamProducer<TempHumidity> implements StreamConsumer<TempHumidity>,
        StreamWindowFormat {

    // threshold used for filtering max temperature values
    private static final int TEMPERATURE_THRESHOLD = 20;
    // window size within which the filtering is applied
    private static final int TEMPERATURE_WINDOW_SIZE = 3;

    @Bean
    public void streamUpper() {
        Serde<TempHumidity> tempHumidityJsonSerde = new JsonSerde<>(TempHumidity.class, objectMapper);
        KStream<String, TempHumidity> source = streamsBuilder.stream(getInputTopicName());
        KStream<String, TempHumidity> max = source
                // temperature values are sent without a key (null), so in order
                // to group and reduce them, a key is needed ("temp" has been chosen)
                .selectKey((key, value) -> "temp")
                .groupByKey(Grouped.with(Serdes.String(), tempHumidityJsonSerde))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)))
                .reduce((value1, value2) -> {
                    if (value2.getTemp() > value1.getTemp()) {
                        return value2;
                    } else {
                        return value1;
                    }
                })
                .toStream()
                //过滤条件就是温度大于20
                .filter((key, value) -> value.getTemp() > TEMPERATURE_THRESHOLD)
                .selectKey((k,v)-> windowedKeyToString(k));
        // need to override key serde to Windowed<String> type
        max.to("iot-temperature-max", Produced.with(Serdes.String(), tempHumidityJsonSerde));
    }

    @Override
    public List<TempHumidity> getData() {
        return Lists.newArrayList(
                TempHumidity.builder().temp(10).humidity(26).build(),
                TempHumidity.builder().temp(10).humidity(26).build(),
                TempHumidity.builder().temp(23).humidity(26).build(),
                TempHumidity.builder().temp(39).humidity(36).build()
        );
    }

    @Override
    public String getInputTopicName() {
        return getClassName(TempHumidity.class);
    }

    @Override
    public String getOutputTopicName() {
        return "iot-temperature-max";
    }
}
