package com.hy.redislazy.zset;

import cn.hutool.core.lang.UUID;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * @author hy
 * @date 2021/11/29 12:08 下午
 * @description
 */
@Slf4j
@Component
public class RedisSetNotify {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final int threadNum = 10;

    /**
     * 生产者,生成5个订单放进去
     */
    public void productionDelayMessage() {
        for (int i = 0; i < 5; i++) {
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime afterThreeSec = now.plusSeconds(3);
            String orderIdStr = "orderId" + i;
            stringRedisTemplate.opsForZSet().add("OrderId", orderIdStr, afterThreeSec.toEpochSecond(ZoneOffset.of("+8")));
            log.info(System.currentTimeMillis() + "ms:redis生成了一个订单任务：订单ID为" + orderIdStr);
        }
    }

    /**
     * 消费者，取订单多线程下重复消费
     */
    public void consumerDelayMessage() {
        while (true) {
            Set<TypedTuple<String>> items = stringRedisTemplate.opsForZSet().rangeWithScores("OrderId", 0, 1);
            if (CollectionUtils.isEmpty(items)) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
            items.stream().findFirst().ifPresent(item -> {
                double score = Optional.ofNullable(item.getScore()).orElse(0d);
                String value = item.getValue();
                long now = LocalDateTime.now().toEpochSecond(ZoneOffset.of("+8"));
                if (now >= score) {
                    Long returnValue = stringRedisTemplate.opsForZSet().remove("OrderId", value);
                    //这里是控制并发的关键
                    Optional.ofNullable(returnValue).filter(one->one>0)
                            .ifPresent(one-> log.info("{} ms:redis消费了一个任务：消费的订单OrderId为{},returnValue:{}",System.currentTimeMillis(),value,returnValue));
                }
            });
        }

    }

    @PostConstruct
    public void init() {
        productionDelayMessage();
        //消费者，取订单多线程下重复消费
//        consumerDelayMessage();
        for(int i=0;i<threadNum;i++){
            CompletableFuture.runAsync(this::consumerDelayMessage);
        }
    }

}
