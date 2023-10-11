package com.an.job.liveStreaming.operation;

import com.an.job.kafka.MyKafkaDeserializationSchema;
import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.liveStreaming.pojo.ItemEventCount;
import com.an.job.udf.HotGoodsAggregateFunction;
import com.an.job.udf.HotGoodsTopNFunction;
import com.an.job.udf.HotGoodsWindowFunction;
import com.an.job.udf.JsonToDataBeanV2;
import com.an.job.util.FlinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

import static com.an.job.constant.EventID.Starts_Product;

public class HotGoodsCount {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> dataStream = FlinkUtil.createKafkaStreamV2(args, MyKafkaDeserializationSchema.class);
        dataStream.process(new JsonToDataBeanV2())
                .filter(bean -> bean.getEventId().startsWith(Starts_Product))
                // 设置无界 流水位线 乱序时间范围
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<DataBean>forBoundedOutOfOrderness(Duration.ofMillis(5000))
                                .withTimestampAssigner(
                                        (bean,ts) -> bean.getTimestamp()
                                )
                )
                .keyBy(new KeySelector<DataBean, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(DataBean bean) throws Exception {
                        return Tuple3.of(
                                bean.getEventId(),
                                bean.getProperties().get("category_id").toString(),
                                bean.getProperties().get("product_id").toString()
                        );
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.minutes(10),Time.minutes(1)))
//                .window(TumblingEventTimeWindows.of(Time.milliseconds(10 * 1000)))
                .aggregate(
                        new HotGoodsAggregateFunction()
                        , new HotGoodsWindowFunction()
                )
                .keyBy(new KeySelector<ItemEventCount, Tuple4<String, String, Long, Long>>() {
                    @Override
                    public Tuple4<String, String, Long, Long> getKey(ItemEventCount value) throws Exception {
                        return Tuple4.of(value.categoryId, value.eventId, value.windowStart, value.windowEnd);
                    }
                })
                .process(new HotGoodsTopNFunction())
                .print()
                ;

        FlinkUtil.getEnv().execute();
    }       
}
