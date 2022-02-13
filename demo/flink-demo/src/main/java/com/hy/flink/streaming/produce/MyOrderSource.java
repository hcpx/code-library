package com.hy.flink.streaming.produce;

import com.hy.flink.streaming.bean.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author hy
 * @date 2022/2/8 7:41 下午
 * @description
 */
@Slf4j
public class MyOrderSource implements SourceFunction<Order> {
    private static final List<String> list;
    static {
        list = new ArrayList<>();
        list.add("UDFOrder");
        list.add("TDFOrder");
    }

    private boolean running = true;

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        final Random random = new Random();
        while (running) {
            Order order = new Order();
            order.setId("order_" + System.currentTimeMillis() % 700);
            order.setPrice(random.nextDouble() * 100);
            int index = random.nextInt(2);
            order.setOrderType(list.get(index));
            order.setTimestamp(System.currentTimeMillis());
            //发送对象
            ctx.collect(order);
//            log.info("source order:{}",order);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
