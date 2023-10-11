package com.an.job.udf;

import ch.hsr.geohash.GeoHash;
import com.an.job.liveStreaming.pojo.DataBean;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class GeoHashProcess
        extends KeyedBroadcastProcessFunction<String, DataBean, Map<String, Tuple3<String, String, String>>, DataBean> {
    // 映射状态
    private final MapStateDescriptor<Void, Map<String, Tuple3<String, String, String>>> broadcastStateDescriptor;

    public GeoHashProcess(MapStateDescriptor<Void, Map<String, Tuple3<String, String, String>>> broadcastStateDescriptor) {
        this.broadcastStateDescriptor = broadcastStateDescriptor;
    }

    @Override
    public void processElement(DataBean value, KeyedBroadcastProcessFunction<String, DataBean, Map<String, Tuple3<String, String, String>>, DataBean>.ReadOnlyContext ctx, Collector<DataBean> out) throws Exception {
        // 从广播状态中获取HashMap数据
        Map<String, Tuple3<String, String, String>> mapState = ctx.getBroadcastState(broadcastStateDescriptor).get(null);

        String geoStr = GeoHash.geoHashStringWithCharacterPrecision(
                value.getLatitude(),
                value.getLongitude(),
                6);
        if (mapState.containsKey(geoStr)) {
            Tuple3<String, String, String> areaInfo = mapState.get(geoStr);
            value.setProvince(areaInfo.f0);
            value.setCity(areaInfo.f1);
            value.setRegion(areaInfo.f2);
            out.collect(value);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, Tuple3<String, String, String>> value, KeyedBroadcastProcessFunction<String, DataBean, Map<String, Tuple3<String, String, String>>, DataBean>.Context ctx, Collector<DataBean> out) throws Exception {
        // 更新广播状态
        BroadcastState<Void, Map<String, Tuple3<String, String, String>>> broadcastState = ctx.getBroadcastState(broadcastStateDescriptor);

        broadcastState.put(null, value);
    }
}
