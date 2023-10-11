package com.an.job.udf;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.an.job.liveStreaming.pojo.DataBean;
import com.an.job.util.log;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class LocationFunction extends RichAsyncFunction<DataBean, DataBean> {

    private transient CloseableHttpAsyncClient httpAsyncClient;
    private final String url;
    private final String key;
    private final int maxConnTotal;


    private MapStateDescriptor<Void, Map<String, Tuple3<String, String, String>>> broadcastStateDescriptor;



    public LocationFunction(String url, String key, int maxConnTotal) {
        this.url = url;
        this.key = key;
        this.maxConnTotal = maxConnTotal;
    }

    public LocationFunction(ParameterTool parameterTool) {
        this.url = parameterTool.getRequired("amap.http.url");
        this.key = parameterTool.getRequired("amap.key");
        this.maxConnTotal = 50;
    }

    public LocationFunction(ParameterTool parameterTool,int maxConnTotal) {
        this.url = parameterTool.getRequired("amap.http.url");
        this.key = parameterTool.getRequired("amap.key");
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 在open方法中初始化广播状态描述符
        broadcastStateDescriptor = new MapStateDescriptor<>(
                "AreaMapBroadcastState",
                Types.VOID,
                Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
        );

        RequestConfig requestConfig = RequestConfig.custom().build();
        httpAsyncClient = HttpAsyncClients.custom()
                .setMaxConnTotal(maxConnTotal)
                .setDefaultRequestConfig(requestConfig).build();
        httpAsyncClient.start();
    }

    @Override
    public void close() throws Exception {
        httpAsyncClient.close();
    }

    @Override
    public void asyncInvoke(DataBean dataBean, ResultFuture<DataBean> resultFuture) throws Exception {
        Double longitude = dataBean.getLongitude();
        Double latitude = dataBean.getLatitude();

        if (!Objects.isNull(dataBean.getProvince()) || !Objects.isNull(dataBean.getCity())){
            // TODO 无需调用
            System.out.println(log.primary("primary 无需调用 ===> "+dataBean.toString()));
            resultFuture.complete(Collections.singleton(dataBean));
            return;
        }

        HttpGet httpGet = new HttpGet(url + "?location=" + longitude + "," + latitude + "&key=" + key);
        Future<HttpResponse> future = httpAsyncClient.execute(httpGet, null);
        CompletableFuture.supplyAsync(new Supplier<DataBean>() {
            @Override
            public DataBean get() {
                HttpResponse response = null;
                try {
                    response = future.get();
                    String province = null;
                    String city = null;
                    if (response.getStatusLine().getStatusCode() == 200){
                        // JSON 解析
                        String result = EntityUtils.toString(response.getEntity());
                        JSONObject jsonObject = JSON.parseObject(result);
                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                        if (regeocode != null && !regeocode.isEmpty()){
                            JSONObject addressComponent = regeocode.getJSONObject("addressComponent");
                            province = addressComponent.getString("province");
                            city = addressComponent.getString("city");
                        }
                    }
                    if (Objects.isNull(province) && Objects.isNull(city)){
                        // TODO API 失效
                        System.out.println(log.warning("warning API 失效 ===> "+dataBean));
                    }else {
                        // TODO API 成功
                        System.out.println(log.success("success API 成功 ===> "+dataBean));
                    }
                    dataBean.setProvince(province);
                    dataBean.setCity(city);
                    return dataBean;
                } catch (InterruptedException | ExecutionException | IOException e) {
                    // TODO 调用API失败
                    System.out.println(log.error("ERROR ===>\t"+ e));
                    System.out.println(log.error("\t"+dataBean));
                }
                return dataBean;
            }
        }).thenAccept((DataBean res) -> {
            resultFuture.complete(Collections.singleton(res));
        });
    }
}
