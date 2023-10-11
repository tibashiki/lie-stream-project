package com.an.job.liveStreaming.operation;

import com.an.job.kafka.MyKafkaDeserializationSchema;
import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.liveStreaming.pojo.LiveGift;
import com.an.job.source.MySqlLiveGiftSource;
import com.an.job.udf.GiftConnectFunction;
import com.an.job.udf.JsonToDataBeanV2;
import com.an.job.util.FlinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.Map;

import static com.an.job.constant.EventID.LIVE_REWARD;

public class GiftPointCount {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> dataStream = FlinkUtil.createKafkaStreamV2(args, MyKafkaDeserializationSchema.class);
        DataStreamSource<LiveGift> giftDataSource = FlinkUtil.getEnv().addSource(new MySqlLiveGiftSource());
//        giftDataSource.print();

        MapStateDescriptor<Integer, LiveGift> stateDescriptor =
                new MapStateDescriptor<>(
                    "gift-broadcast-state",
                    TypeInformation.of(new TypeHint<Integer>() {}),
                    TypeInformation.of(new TypeHint<LiveGift>() {})
                );
        BroadcastStream<LiveGift> broadcast = giftDataSource.broadcast(stateDescriptor);

        dataStream.process(new JsonToDataBeanV2())
                .filter((FilterFunction<DataBean>) bean -> LIVE_REWARD.equals(bean.getEventId()))
                .connect(broadcast)
                .process(new GiftConnectFunction(stateDescriptor))
                .keyBy(tuple2 -> tuple2.f0)
                .reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> reduce(Tuple3<String, String, Double> t1, Tuple3<String, String, Double> t2) throws Exception {
                        return Tuple3.of(t1.f0,t1.f1+"_"+t2.f1,t1.f2+t2.f2);
                    }
                })
                .print();

        FlinkUtil.getEnv().execute();
    }
}
