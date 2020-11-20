package com.hy.async.async;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * @author : tangjian
 * @date : 2020/11/17 1:30 下午
 */
@Service
public class AsyncExecutor {

    @Async
    public void doExecute(Runnable runnable) {
        runnable.run();
    }
}
