package com.hy.flink.streaming.sink;

import com.hy.flink.streaming.bean.Order;
import com.hy.flink.streaming.produce.MyOrderSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hy
 * @date 2022/2/8 4:33 下午
 * @description
 */
@Slf4j
public class JDBCSinkFunction {
    //中间出现异常暂时没相当处理办法 RickSink可能是个更好的选择
    private static final String URL = "jdbc:mysql://localhost:3307/heyue?useUnicode=true&characterEncoding=UTF-8";
    private static final String USER_NAME = "root";
    private static final String PWD = "123456";
    private static final String INSERT = "insert into flink_order (id, price,order_type) values (?, ?, ?) ON DUPLICATE KEY UPDATE price=? ";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Order> source = env.addSource(new MyOrderSource());
        source.addSink(JdbcSink.sink(
                INSERT,
                (ps, t) -> {
                    log.info("data:{}", t);
                    ps.setString(1, t.getId());
                    ps.setDouble(2, t.getPrice());
                    ps.setString(3, t.getOrderType());
                    ps.setDouble(4, t.getPrice());
                },JdbcExecutionOptions.builder().withBatchIntervalMs(100000).withBatchSize(10).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(URL)
                        .withUsername(USER_NAME)
                        .withPassword(PWD)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .build()));

        env.execute("UDFJDBCSinkDemo");
    }
}
