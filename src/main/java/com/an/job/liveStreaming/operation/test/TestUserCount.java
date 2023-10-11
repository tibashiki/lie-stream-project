package com.an.job.liveStreaming.operation.test;


import ch.hsr.geohash.GeoHash;
import com.an.job.constant.EventID;
import com.an.job.kafka.MyKafkaDeserializationSchema;
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
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestUserCount {

    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> dataStream = FlinkUtil.createKafkaStreamV2(args, MyKafkaDeserializationSchema.class);
        Map<String, Tuple3<String,String,String>> AreaMap = new HashMap<>();
        // 添加数据到AreaMap
        List<AreaDict> dataBeans = new JsonUtil<AreaDict>().readListFile("conf/dim_area_dict.json", AreaDict.class);
        for (AreaDict area : dataBeans) {
            AreaMap.put(area.getGeohash(),Tuple3.of(area.getProvince(),area.getCity(),area.getRegion()));
        }


        DataStream<String> kafkaStream = FlinkUtil.createKafkaStream(args, SimpleStringSchema.class);


        SingleOutputStreamOperator<DataBean> DataBeans = kafkaStream
                .process(new JsonToDataBean())
                .filter(bean -> EventID.APP_LAUNCH.equals(bean.getEventId()))
                .keyBy(DataBean::getDeviceId)
                .map(new MapFunction<DataBean, DataBean>() {
                    @Override
                    public DataBean map(DataBean value) throws Exception {
                        String geoStr = GeoHash.geoHashStringWithCharacterPrecision(
                                value.getLatitude(),
                                value.getLongitude(),
                                6);
                        if (AreaMap.containsKey(geoStr)) {
                            Tuple3<String, String, String> areaInfo = AreaMap.get(geoStr);
                            value.setProvince(areaInfo.f0);
                            value.setCity(areaInfo.f1);
                            value.setRegion(areaInfo.f2);
                        }
                        return value;
                    }
                });
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
