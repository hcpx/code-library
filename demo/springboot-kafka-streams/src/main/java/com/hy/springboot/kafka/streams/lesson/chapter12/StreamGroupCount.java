package com.hy.springboot.kafka.streams.lesson.chapter12;

import cn.hutool.core.util.IdUtil;
import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.MessageEvent;
import com.hy.springboot.kafka.streams.domain.MessageEventArr;
import com.hy.springboot.kafka.streams.domain.StatisticDO;
import com.hy.springboot.kafka.streams.lesson.common.StreamConsumer;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author hy
 * @date 2022/1/21 7:51 下午
 * @description
 */
@Slf4j
@Configuration("streamImpl")
public class StreamGroupCount extends AbstractStreamProducer<MessageEventArr> implements StreamConsumer<StatisticDO> {

    protected ForeachAction<String, Long> foreachAction = (key, value) -> log.info("key:{} value:{}", key, value);

    @Bean
    public void streamUpper() {
        KStream<String, MessageEventArr> stream = streamsBuilder.stream(getInputTopicName());
        stream.flatMapValues(MessageEventArr::getName)
                .peek((key, value) -> log.info("# statistic #" + key + " : " + value))
                .groupBy((key, word) -> word)
                .count()
                .toStream()
                .peek(foreachAction);
    }

    @Override
    public String getInputTopicName() {
        return getClassName(MessageEventArr.class);
    }

    @Override
    public List<MessageEventArr> getData() {
        return Lists.newArrayList(
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy1")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy1")).build(),
                MessageEventArr.builder().id(IdUtil.fastSimpleUUID()).name(Lists.newArrayList("hy", "wy")).build()
        );
    }

    @Override
    public String getOutputTopicName() {
        return getClassName(StatisticDO.class);
    }
}
