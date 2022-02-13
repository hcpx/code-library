package com.hy.flink.streaming.transform;

import com.hy.flink.streaming.bean.Order;
import com.hy.flink.streaming.produce.MyOrderSource;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hy
 * @date 2022/2/8 7:40 下午
 * @description
 */
public class BasicTransform {

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<Order> source = env.addSource(new MyOrderSource());
        //max 下标形式的只支持tuple
        source.keyBy(Order::getOrderType).max("price").print("max");
        source.keyBy(Order::getOrderType).maxBy("price").print("max by");
        env.execute("key by");
    }
}
