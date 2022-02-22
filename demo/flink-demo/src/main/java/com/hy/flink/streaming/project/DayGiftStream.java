package com.hy.flink.streaming.project;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.IdUtil;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.util.Collector;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink.Builder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.IterableUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

/**
 * @author hy
 * @date 2022/2/18 7:54 下午
 * @description
 */
@Slf4j
public class DayGiftStream {

    private static final String TOPIC = "giftRecord";
    private static final String INTERVAL = "_";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.getConfig().setAutoWatermarkInterval(1000);
//        environment.setStateBackend(new RocksDBStateBackend("hdfs://"));
        CompletableFuture.runAsync(DayGiftStream::kafkaProducer);
        final FlinkKafkaConsumer<GiftRecord> mySource = new FlinkKafkaConsumer<>(TOPIC, getSerializationSchema(environment), getProperties());
        mySource.setStartFromLatest();
//        environment.addSource(mySource).print("kafka producer");
        SingleOutputStreamOperator<FansGiftResult> aggregate = environment.addSource(mySource)
                .filter(giftRecord -> Objects.nonNull(giftRecord.getHostId()))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<GiftRecord>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                        .withTimestampAssigner((GiftRecord element, long recordTimestamp) -> Optional
                                .ofNullable(element)
                                .map(GiftRecord::getGiftTime)
                                .map(time -> time.toInstant(ZoneOffset.of("+8")).toEpochMilli())
                                .orElse(LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli())))
                .keyBy(giftRecord -> giftRecord.getHostId() + INTERVAL + giftRecord.getFansId())
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new MyAggregate(), new MyWindowFunction());

        List<HttpHost> list = Lists.newArrayList(new HttpHost("localhost", 19200));
        Builder<FansGiftResult> esBuilder = new ElasticsearchSink.Builder<>(list, (ElasticsearchSinkFunction<FansGiftResult>) (giftRecord, runtimeContext, requestIndexer) -> {
            IndexRequest data = Requests.indexRequest()
                    .index("daygiftanalyze")
                    .type("_doc")
                    .id(IdUtil.fastSimpleUUID())
                    .source(BeanUtil.beanToMap(giftRecord));
            requestIndexer.add(data);
        });
        esBuilder.setBulkFlushMaxActions(1);//每次写入
        esBuilder.setBulkFlushBackoffRetries(3);//重试三次
        aggregate.print("windowPrint");
        aggregate.addSink(esBuilder.build());
        environment.execute("daygiftanalyze");
    }

    @SneakyThrows
    private static void kafkaProducer() {
        log.info("kafkaProducerWithJson start");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<GiftRecord> stream = env.fromCollection(createData());

        FlinkKafkaProducer<GiftRecord> myProducer = new FlinkKafkaProducer<>(
                TOPIC,                  // 目标 topic
                getSerializationSchema(env),     // 序列化 schema
                getProperties()); // 配置
        stream.addSink(myProducer);
        Thread.sleep(10000);
        log.info("kafkaProducerWithJson end");
        env.execute("kafkaProducerWithJson");
    }

    /**
     * 输入 累计值即是总数 输出值
     */
    private static class MyAggregate implements AggregateFunction<GiftRecord, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(GiftRecord value, Long accumulator) {
            return accumulator + value.getGiftCount();
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class MyWindowFunction extends RichWindowFunction<Long, FansGiftResult, String, TimeWindow> {

        ValueState<FansGiftResult> state;

        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<FansGiftResult> out) throws Exception {
            String[] split = key.split(INTERVAL);
            long end = window.getEnd();
            out.collect(FansGiftResult.builder()
                    .hostId(split[0])
                    .fansId(split[1])
                    .giftCount(IterableUtils.toStream(input)
                            .reduce(Long::sum)
                            .orElse(0L))
                    .windowsEnd(end)
                    .windowsEndStr(DateUtil.formatDateTime(DateUtil.date(end))).build());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<FansGiftResult> valueStateDescriptor = new ValueStateDescriptor<>("MyWindowFunction", FansGiftResult.class);
            state = this.getRuntimeContext().getState(valueStateDescriptor);
        }

    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        return properties;
    }

    private static TypeInformationSerializationSchema<GiftRecord> getSerializationSchema(StreamExecutionEnvironment env) {
        return new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<GiftRecord>() {
        }), env.getConfig());
    }

    private static List<GiftRecord> createData() {
        List<GiftRecord> giftRecords = Lists.newArrayList();
        List<String> hostIds = Lists.newArrayList("hy", "wx", "wy");
        List<String> fansIds = Lists.newArrayList(IdUtil.fastSimpleUUID(), IdUtil.fastSimpleUUID(), IdUtil.fastSimpleUUID());
        ThreadLocalRandom current = ThreadLocalRandom.current();
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 16; i++) {
            GiftRecord giftRecord = GiftRecord.builder()
                    .hostId(hostIds.get(current.nextInt(hostIds.size())))
                    .fansId(fansIds.get(current.nextInt(fansIds.size())))
                    .giftCount(current.nextLong(1000))
                    .giftTime(now)
                    .build();
            now = now.plusMinutes(2);
            giftRecords.add(giftRecord);
        }
        giftRecords.forEach(giftRecord -> log.info("input:{}", giftRecord));
        return giftRecords;
    }
}
