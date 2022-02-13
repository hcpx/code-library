package com.hy.flink.streaming.window;

import com.hy.flink.streaming.bean.Order;
import com.hy.flink.streaming.produce.MyOrderSource;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hy
 * @date 2022/2/9 10:04 下午
 * @description
 */
public class CountWindowDemo {

    @SneakyThrows
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<Order> source = env.addSource(new MyOrderSource());
        //每进入2个元数最对最近的5个元数进行计算
        source.filter(order->order.getOrderType().equals("TDFOrder")).keyBy(Order::getOrderType).countWindow(5,2).maxBy("price").print("maxByPrice");
        env.execute("max by price");
    }
}
