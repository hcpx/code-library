package com.hy.redislazy.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/20 11:42
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class RedisTest {
    private final RedisTemplate<String,Object> redisTemplate;

    @PostConstruct
    public void test() {
        // 调用 redisTemplate 对象设置一个10s 后过期的键，不出意外 10s 后键过期后会触发事件打印结果
        redisTemplate.boundValueOps("job").set("10s", 10, TimeUnit.SECONDS);
        log.info("begin = {}", LocalDateTime.now());
    }
}
