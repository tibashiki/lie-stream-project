package com.an.job.udf;

import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.liveStreaming.pojo.LiveGift;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class GiftConnectFunction extends BroadcastProcessFunction<DataBean, LiveGift, Tuple3<String,String,Double>> {
    private MapStateDescriptor<Integer, LiveGift> stateDescriptor;

    public GiftConnectFunction(MapStateDescriptor<Integer, LiveGift> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void processElement(DataBean bean, BroadcastProcessFunction<DataBean, LiveGift, Tuple3<String, String, Double>>.ReadOnlyContext ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
        ReadOnlyBroadcastState<Integer, LiveGift> broadcastState = ctx.getBroadcastState(stateDescriptor);
        String anchorId = bean.getProperties().get("anchor_id").toString();
        Integer giftId = Integer.parseInt(bean.getProperties().get("gift_id").toString());
        if (broadcastState.contains(giftId)){
            LiveGift liveGift = broadcastState.get(giftId);
            out.collect(Tuple3.of(anchorId,liveGift.getName(),liveGift.getPoints()));
        }
        else {
            out.collect(Tuple3.of(anchorId,giftId.toString(),null));
        }
    }

    @Override
    public void processBroadcastElement(LiveGift gift, BroadcastProcessFunction<DataBean, LiveGift, Tuple3<String, String, Double>>.Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
        BroadcastState<Integer, LiveGift> broadcastState = ctx.getBroadcastState(stateDescriptor);
        broadcastState.remove(gift.getId());
        if (gift.getDeleted() == 0)
            broadcastState.put(gift.getId(),gift);
    }
}
