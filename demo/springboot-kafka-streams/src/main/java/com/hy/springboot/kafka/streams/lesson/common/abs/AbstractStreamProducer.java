package com.hy.springboot.kafka.streams.lesson.common.abs;

import cn.hutool.core.util.IdUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hy.springboot.kafka.streams.lesson.common.StreamProducer;
import java.util.List;
import java.util.Optional;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.CollectionUtils;

/**
 * @author hy
 * @date 2022/1/16 10:57 上午
 */
@Slf4j
public abstract class AbstractStreamProducer<T> implements StreamProducer<T> {

    @Resource
    protected KafkaTemplate<String, T> kafkaTemplate;

    @Resource
    protected StreamsBuilder streamsBuilder;

    @Autowired
    protected ObjectMapper objectMapper;

    protected List<T> data;

    protected boolean isFirst = true;

    public abstract List<T> getData();

    @Scheduled(fixedRate = 2000)
    public void send() {
        if (isFirst) {
            data = getData();
            isFirst = false;
        }
        if (CollectionUtils.isEmpty(data)) {
            return;
        }
        T remove = data.get(0);
        Optional.ofNullable(remove)
                .ifPresent(one -> kafkaTemplate.send(getInputTopicName(), IdUtil.fastSimpleUUID(),
                        remove));
        data.remove(0);
        if (data.size() == 0) {
            log.info("send end!");
        }
    }

}
