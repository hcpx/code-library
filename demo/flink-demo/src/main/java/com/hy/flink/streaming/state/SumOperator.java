package com.hy.flink.streaming.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author hy
 * @Date 2022-02-13 20:20:30
 * @Description 非键空状态的程序运行时即可拿数据
 */
public class SumOperator {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        final DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);
        final SingleOutputStreamOperator<Integer> stream2 = stream.map(new MySumMapper("mySumMapper"));
        stream2.print();
        env.execute("stream");
    }

    //带状态的求和算子
    public static class MySumMapper implements MapFunction<Integer, Integer>, CheckpointedFunction {
        private final String stateKey;
        private int sum;
        //状态
        private ListState<Integer> checkPointedState;

        public MySumMapper(String stateKey) {
            this.stateKey = stateKey;
        }

        //计算方法
        @Override
        public Integer map(Integer value) {
            return sum += value;
        }

        //算子保存状态的方法
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkPointedState.clear();
            checkPointedState.add(sum);
        }

        //算子启动时会加载状态
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>(
                    stateKey,
                    TypeInformation.of(new TypeHint<Integer>() {
                    }));
//            getBroadcastState()   每个并行度状态一样时广播
//            getUnionListState()   等到计算的时候才具体分配
            //并行度确认后就确认了分配策略
            checkPointedState = context.getOperatorStateStore().getListState(descriptor);
            if (context.isRestored()) {
                //和并行度有关
                for (Integer subSum : checkPointedState.get()) {
                    sum += subSum;
                }
            }
        }
    }
}


