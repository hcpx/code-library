package com.hy.flink.streaming.Util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author hy
 * @date 2022/2/8 12:09 下午
 * @description
 */
public class GenericSuperclassUtil {
    /*
     * 获取泛型类Class对象，不是泛型类则返回null
     */
    public static Class<?> getActualTypeArgument(Class<?> clazz) {
        Class<?> entityClass = null;
        Type genericSuperclass = clazz.getGenericSuperclass();
        if (genericSuperclass instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) genericSuperclass)
                    .getActualTypeArguments();
            if (actualTypeArguments != null && actualTypeArguments.length > 0) {
                entityClass = (Class<?>) actualTypeArguments[0];
            }
        }

        return entityClass;
    }
}
