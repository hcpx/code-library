package com.hy.async.async;

import com.alibaba.fastjson.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author : tangjian
 * @date : 2020/11/16 8:04 下午
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AsyncManagerService {

    private final RedisTemplate<String, Object> objectRedisTemplate;


    private final AsyncExecutor asyncExecutor;


    public <T, R> String start(AsyncType asyncType, int batchSize, List<T> params, Function<List<T>, List<R>> function) {
        String progressId = UUID.randomUUID().toString();
        String progressKey = getProgressKey(asyncType, progressId);

        //在redis中新增异步信息
        HashOperations<String, Object, Object> opsForHash = objectRedisTemplate.opsForHash();


        int total = params.size();
        opsForHash.put(progressKey, "total", total);
        opsForHash.put(progressKey, "finished", 0);
        opsForHash.put(progressKey, "finish", false);
        objectRedisTemplate.expire(progressKey, 30, TimeUnit.MINUTES);

        //提交异步任务
        asyncExecutor.doExecute(() -> {
            List<R> data = new ArrayList<>();
            String message = null;
            try {
                int start;
                int end;

                int count = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;

                for (int i = 0; i < count; i++) {
                    start = i * batchSize;
                    end = start + batchSize;

                    if (end > total) {
                        end = total;
                    }
                    List<R> apply = function.apply(params.subList(start, end));
                    if (apply != null && !apply.isEmpty()) {
                        data.addAll(apply);
                    }
                    this.increase(progressKey, end - start);
                }

            } catch (Exception e) {
                log.error("异步执行异常", e);
                message = "执行错误";
            }
            this.finishAll(progressKey, message, data);
        });

        return progressId;
    }

    public void increase(String progressKey, long count) {
        objectRedisTemplate.opsForHash().increment(progressKey, "finished", count);
    }


    private String getProgressKey(AsyncType asyncType, String operateId) {
        return asyncType.getName() + operateId;
    }


    public void finishAll(String progressKey, String message, List<?> data) {
        objectRedisTemplate.opsForHash().put(progressKey, "finish", true);
        if (message != null) {
            objectRedisTemplate.opsForHash().put(progressKey, "message", message);
        } else {
            objectRedisTemplate.opsForHash().delete(progressKey, "message");
        }
        if (CollectionUtils.isNotEmpty(data)) {
            objectRedisTemplate.opsForHash().put(progressKey, "data", data);
        } else {
            objectRedisTemplate.opsForHash().delete(progressKey, "data");
        }
    }

    public ProgressModel getProgress(AsyncType asyncType, String progressId) {
        String progressKey = getProgressKey(asyncType, progressId);
        Map<Object, Object> entries = objectRedisTemplate.opsForHash().entries(progressKey);

        ProgressModel result;
        if (!entries.isEmpty()) {
            result = JSON.parseObject(JSON.toJSONString(entries), ProgressModel.class);
            if (result.isFinish()) {
                objectRedisTemplate.delete(progressKey);
            }
            return result;
        } else {
            throw new RuntimeException("进度已结束或不存在");
        }
    }


}
