package com.an.job.udf;

import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.liveStreaming.pojo.ItemEventCount;
import com.an.job.util.log;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class HotGoodsWindowFunction implements WindowFunction<Long, ItemEventCount, Tuple3<String, String, String>, TimeWindow> {

    @Override
    public void apply(Tuple3<String, String, String> key, TimeWindow window, Iterable<Long> input, Collector<ItemEventCount> out) throws Exception {
        StringBuffer sb = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sb.append("\t[").append(sdf.format(window.getStart())).append(" - ").append(sdf.format(window.getEnd())).append("]\n\t--\t");
        ItemEventCount itemEventCount = new ItemEventCount(
                key.f2,
                key.f0,
                key.f1,
                input.iterator().next(),
                window.getStart(),
                window.getEnd()
        );

        sb.append(itemEventCount.toString()).append("\n");
//        System.out.println(log.primary(sb.toString()));
        out.collect(
                itemEventCount
        );
    }
}
