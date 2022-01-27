package com.hy.springboot.kafka.streams.lesson.common;

import cn.hutool.core.date.DateUtil;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * @author hy
 * @date 2022/1/22 8:25 下午
 * @description
 */
public interface StreamWindowFormat {
    default String windowedKeyToString(Windowed<String> key) {
        return String.format("[%s@%s/%s]", key.key(), DateUtil.date(key.window().start()).toStringDefaultTimeZone(), DateUtil
                .date(key.window().end())
                .toStringDefaultTimeZone());
    }
}
