package com.hy.flink.streaming.sink;

import com.hy.flink.streaming.bean.Order;
import com.hy.flink.streaming.produce.MyOrderSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author hy
 * @date 2022/2/8 4:33 下午
 * @description
 */
@Slf4j
public class RickSinkFunction {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final DataStreamSource<Order> source = env.addSource(new MyOrderSource());
        source.addSink(new MyJDBCSink());

        env.execute("UDFJDBCSinkDemo");
    }

    public static class MyJDBCSink extends RichSinkFunction<Order> {

        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3307/heyue", "root", "123456");
            insertStmt = connection.prepareStatement("insert into flink_order (id, price,order_type) values (?, ?, ?)");
            updateStmt = connection.prepareStatement("update flink_order set price = ?,order_type = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            if (Objects.nonNull(insertStmt)) {
                insertStmt.close();
            }
            if (Objects.nonNull(updateStmt)) {
                updateStmt.close();
            }
            if (Objects.nonNull(connection)) {
                connection.close();
            }
        }

        @Override
        public void invoke(Order value, Context context) throws Exception {
            log.info("更新记录 ： " + value);
            updateStmt.setDouble(1, value.getPrice());
            updateStmt.setString(2, value.getOrderType());
            updateStmt.setString(3, value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getPrice());
                insertStmt.setString(3, value.getOrderType());
                insertStmt.execute();
            }
        }
    }


}
