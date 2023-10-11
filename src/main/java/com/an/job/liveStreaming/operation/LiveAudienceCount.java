package com.an.job.liveStreaming.operation;

import com.an.job.kafka.MyKafkaDeserializationSchema;
import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.udf.AnchorDistinctTotalAudienceFunc;
import com.an.job.udf.JsonToDataBeanV2;
import com.an.job.util.FlinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Objects;

import static com.an.job.constant.EventID.LIVE_ENTER;
import static com.an.job.constant.EventID.LIVE_LEAVE;

public class LiveAudienceCount {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> dataStream = FlinkUtil.createKafkaStreamV2(args, MyKafkaDeserializationSchema.class);
        // 直播相关的数据
        SingleOutputStreamOperator<DataBean> live = dataStream.process(new JsonToDataBeanV2())
                .filter(new FilterFunction<DataBean>() {
                    @Override
                    public boolean filter(DataBean bean) throws Exception {
                        return bean.getEventId().startsWith("live");
                    }
                });
        // 进入直播 离开直播相关的数据
        SingleOutputStreamOperator<DataBean> filter = live.filter(bean -> LIVE_ENTER.equals(bean.getEventId()) || LIVE_LEAVE.equals(bean.getEventId()));


        AnchorDistinctTotalAudienceFunc audienceFunc = new AnchorDistinctTotalAudienceFunc();
        // 统计 -pv -uv -直播间在线人数
        SingleOutputStreamOperator<DataBean> mainStream = filter
                .keyBy(bean -> bean.getProperties().get("anchor_id").toString())
                .process(audienceFunc);
        // TODO 案例 输入 ClickHouse 案例
        // 输出到 ClickHouse
        mainStream.addSink(JdbcSink.sink(
                "insert into tb_anchor_audience_count (id,anchor_id,deviceId,eventId,os,province,channel,deviceType,eventTime,date,hour) values (?,?,?,?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setString(2,t.getProperties().get("anchor_id").toString());
                    ps.setString(3, t.getDeviceId());
                    ps.setString(4, t.getEventId());
                    ps.setString(5,t.getOsName());
                    ps.setString(6, t.getProvince());
                    ps.setString(7, t.getReleaseChannel());
                    ps.setString(8, t.getDeviceType());
                    ps.setLong(9, t.getTimestamp());
                    ps.setString(10,t.getDate());
                    ps.setString(11,t.getHour());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(FlinkUtil.getParameterTool().getInt("clickhouse.batch.size"))
                        .withBatchIntervalMs(FlinkUtil.getParameterTool().getInt("clickhouse.batch.interval"))
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://node01:8123/liveStreaming")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .withPassword("123456")
                        .build()));


        // TODO     案例 测输出流的使用
        // 获取侧输出流 
        DataStream<Tuple4<String, Integer, Integer, Integer>> aggDataStream = mainStream.getSideOutput(audienceFunc.getAggTag());

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("node02")
                .setPassword("123456")
                .setDatabase(3)
                .build();
        // TODO 案例 输出到 redis
        //输出到 Redis
        aggDataStream.addSink(new RedisSink<>(jedisPoolConfig,new AudiceRedisMapper()));


        // 统计人气值
        filter
                .keyBy(new KeySelector<DataBean, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DataBean bean) throws Exception {
                        String deviceId = bean.getDeviceId();
                        String anchorId = bean.getProperties().get("anchor_id").toString();
                        return Tuple2.of(deviceId,anchorId);
                    }
                })
                .process(new KeyedProcessFunction<Tuple2<String, String>, DataBean, Tuple2<String,Integer>>() {
                    private transient ValueState<Long> inState;
                    private transient ValueState<Long> outState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Long> inStateDescriptor = new ValueStateDescriptor<>("in-state", TypeInformation.of(new TypeHint<Long>() {}));
                        inState = getRuntimeContext().getState(inStateDescriptor);

                        ValueStateDescriptor<Long> outStateDescriptor = new ValueStateDescriptor<>("out-state", TypeInformation.of(new TypeHint<Long>() {}));
                        outState = getRuntimeContext().getState(outStateDescriptor);
                    }

                    @Override
                    public void processElement(DataBean bean, KeyedProcessFunction<Tuple2<String, String>, DataBean, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Long onlineTime = 60 * 1000L;
                        String eventId = bean.getEventId();
                        Long timestamp = bean.getTimestamp();


                        if (LIVE_ENTER.equals(eventId)){
                            inState.update(timestamp);
                            // onTimer 触发器
                            ctx.timerService().registerProcessingTimeTimer(timestamp + onlineTime + 1 );
                        }else {
                            Long inTime = inState.value();
                            if (Objects.isNull(inTime))
                                return;
                            outState.update(inTime);
                            if (timestamp - inTime < onlineTime){
                                ctx.timerService().deleteEventTimeTimer(inTime + onlineTime + 1);
                            }
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<String, String>, DataBean, Tuple2<String, Integer>>.OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 超时后返回输出结果
                        // 或者 超过30 分钟中输出结果
                        Long outTime = outState.value();
                        if (Objects.isNull(outTime))
                            out.collect(Tuple2.of(ctx.getCurrentKey().f0,1));
                        else {
                            Long inTime = inState.value();
                            if (inTime - outTime > 30 * 6000)
                                out.collect(Tuple2.of(ctx.getCurrentKey().f0,1));
                        }
                    }
                })
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1);


        FlinkUtil.getEnv().execute();
    }

    // TODO 案例 Redis 的 Mapper 构建
    public static class AudiceRedisMapper implements RedisMapper<Tuple4<String,Integer,Integer,Integer>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"audience-count");
        }

        @Override
        public String getKeyFromData(Tuple4<String, Integer, Integer, Integer> tuple4) {
            return tuple4.f0 ;
        }

        @Override
        public String getValueFromData(Tuple4<String, Integer, Integer, Integer> tuple4) {
            return tuple4.f1+","+tuple4.f2+","+tuple4.f3;
        }
    }
}
