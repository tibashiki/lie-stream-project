package com.an.job.liveStreaming.operation;

import com.an.job.constant.EventID;
import com.an.job.liveStreaming.pojo.AreaDict;
import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.udf.GeoHashProcess;
import com.an.job.udf.IsNewUserFunction;
import com.an.job.udf.JsonToDataBean;
import com.an.job.udf.LocationFunction;
import com.an.job.util.FlinkUtil;
import com.an.job.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class UserCount {
    public static void main(String[] args) throws Exception {
        Map<String, Tuple3<String,String,String>> AreaMap = new HashMap<>();
        // 添加数据到AreaMap
        final List<AreaDict> dataBeans = new JsonUtil<AreaDict>().readListFile("conf/dim_area_dict.json", AreaDict.class);
        for (AreaDict area : dataBeans) {
            AreaMap.put(area.getGeohash(),Tuple3.of(area.getProvince(),area.getCity(),area.getRegion()));
        }


        DataStream<String> kafkaStream = FlinkUtil.createKafkaStream(args, SimpleStringSchema.class);
        kafkaStream.print();

        // 创建一个描述符来定义广播状态
        MapStateDescriptor<Void, Map<String, Tuple3<String, String, String>>> broadcastStateDescriptor =
                new MapStateDescriptor<>("broadcastState",
                    TypeInformation.of(new TypeHint<Void>() {}), // 这里使用TypeHint明确指定类型信息
                    TypeInformation.of(new TypeHint<Map<String,Tuple3<String,String,String>>>() {})
                );

        // 创建广播流并广播 HashMap 数据
        BroadcastStream<Map<String, Tuple3<String, String, String>>> broadcastStream = FlinkUtil.getEnv()
                .fromCollection(Collections.singletonList(AreaMap))
                .broadcast(broadcastStateDescriptor);

        SingleOutputStreamOperator<DataBean> DataBeans = kafkaStream
                .process(new JsonToDataBean())
                .filter(bean -> EventID.APP_LAUNCH.equals(bean.getEventId()))
                .keyBy(DataBean::getDeviceId)
                .connect(broadcastStream)
                .process(new GeoHashProcess(broadcastStateDescriptor))
                .map(new IsNewUserFunction());
//        DataBeans.print();


        AsyncDataStream
                .unorderedWait(
                DataBeans,
                new LocationFunction(FlinkUtil.getParameterTool()),
                5,
                TimeUnit.SECONDS
        )
                .map(new MapFunction<DataBean, Tuple3<String,Integer,Integer>>() {
                         @Override
                         public Tuple3<String, Integer, Integer> map(DataBean dataBean) throws Exception {
                             return Tuple3.of(dataBean.getProvince(),dataBean.getIsN(),1);
                         }
                     }
                )
                .keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> in) throws Exception {
                        return Tuple2.of(in.f0,in.f1);
                    }
                })
                .sum(2)
                .print();

        FlinkUtil.getEnv().execute();
    }
}
