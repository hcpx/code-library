package com.hy.redislazy.config;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/20 11:38
 */

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.time.LocalDateTime;

/**
 * 当redis 中的key过期时，触发一个事件，并不会准点触发事件，适用于时间不是特别敏感的触发需求。
 * 我们可以算好需要执行的时间间隔作为key失效时间，这样就可以保证到点执行逻辑了。
 */
@Slf4j
public class RedisJobEventMessageListener extends KeyExpirationEventMessageListener {
    /**
     * Instantiates a new Redis event message listener.
     *
     * @param listenerContainer the listener container
     */
    public RedisJobEventMessageListener(RedisMessageListenerContainer listenerContainer) {
        super(listenerContainer);
    }


    @Override
    protected void doHandleMessage(Message message) {
        String key = message.toString();
        // 这个就是过期的key ，过期后，也就是事件触发后对应的value是拿不到的。
        // 这里实现业务逻辑，如果是服务器集群的话需要使用分布式锁进行抢占执行。
        log.info("key = {}", key);
        log.info("end = {}", LocalDateTime.now());
    }

}
