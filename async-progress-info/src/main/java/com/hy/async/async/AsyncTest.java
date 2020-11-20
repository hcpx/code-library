package com.hy.async.async;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @description:
 * @author: hy
 * @create: 2020/11/18 14:26
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AsyncTest {

    private final AsyncManagerService asyncManagerService;

    @Resource
    AsyncTest asyncTest;

    @PostConstruct
    public void test() {
        List<String> strs = IntStream.iterate(0, i -> i + 1).limit(100).boxed().map(String::valueOf).collect(Collectors.toList());
        String asyncName = asyncManagerService.start(AsyncType.normal, 10, strs, (one) -> {
            one.forEach(two -> {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log.info("this program thread is :{}", Thread.currentThread().getName());
            });
            return one;
        });
        asyncTest.count(asyncName);
    }

    @Async
    public void count(String asyncName) {
        ProgressModel progress = asyncManagerService.getProgress(AsyncType.normal, asyncName);
        while (!progress.isFinish()) {
            progress = asyncManagerService.getProgress(AsyncType.normal, asyncName);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("asyncName {} finish info:{}", asyncName, progress.toString());
        }
    }
}
