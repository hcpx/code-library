package com.hy.flink.streaming.source;

import com.hy.flink.streaming.bean.Order;
import com.hy.flink.streaming.produce.MyOrderSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hy
 * @date 2022/2/8 2:08 下午
 * @description
 */
public class UDFSource {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<Order> orderDataStreamSource = env.addSource(new MyOrderSource());
        orderDataStreamSource.print();
        env.execute("UDFOrderSource");
    }

}
