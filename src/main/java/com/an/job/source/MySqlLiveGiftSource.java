package com.an.job.source;

import com.an.job.liveStreaming.pojo.LiveGift;
import com.an.job.util.FlinkUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class MySqlLiveGiftSource extends RichSourceFunction<LiveGift> {
    private Connection connection = null;

    PreparedStatement preparedStatement = null;
    private boolean flag = true;
    @Override
    public void open(Configuration parameters) throws Exception {
        String url = FlinkUtil.getParameterTool().get("jdbc.mysql.url","jdbc:mysql://node01:3306/test?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false");
        String user = FlinkUtil.getParameterTool().get("jdbc.mysql.user", "root");
        String password = FlinkUtil.getParameterTool().get("jdbc.mysql.password", "123456");
        connection = DriverManager.getConnection(url, user, password);
    }

    @Override
    public void run(SourceContext<LiveGift> ctx) throws Exception {
        long timeStamp = 0l;
        while (flag){
            String sql = "SELECT\n" +
                    "    id,`name`,points,deleted \n" +
                    "FROM \n" +
                    "    tb_live_gift \n" +
                    "where updateTime > ?"+ (timeStamp == 0 ?" AND deleted = 0;":"");
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setDate(1,new Date(timeStamp));
            ResultSet resultSet = preparedStatement.executeQuery();
            timeStamp = System.currentTimeMillis();
            LiveGift liveGift = new LiveGift();
            while (resultSet.next()){
                liveGift.setId(resultSet.getInt("id"));
                liveGift.setName(resultSet.getString("name"));
                liveGift.setPoints(resultSet.getDouble("points"));
                liveGift.setDeleted(resultSet.getInt("deleted"));
                ctx.collect(liveGift);
            }
            resultSet.close();
            Thread.sleep(10*1000);
        }

    }

    /**
     * 取消
     */
    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * 关闭
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        preparedStatement.close();
        connection.close();
    }
}
