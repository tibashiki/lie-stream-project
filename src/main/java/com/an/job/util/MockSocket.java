package com.an.job.util;


import com.alibaba.fastjson.JSON;
import com.an.job.common.SocketProp;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * source 自动化测试
 */
public class MockSocket {
    static ServerSocket serverSocket = null;
    static PrintWriter pw = null;
    static Integer count = 0;
    static Integer sum = 0;
    static String masterKey = "";
    static long sleepTime = 500L;
    static String fluctuation = "no";
    static HashMap<String, Tuple2<String,String>> MetaDataMap = new HashMap<String, Tuple2<String,String>>() {
        {
            put("--type", Tuple2.of("kv", "\t默认类型\t\t->\t默认值：'kv' \\ 可选：'json' \\ 'input(键盘输入)'"));
            put("--keylength", Tuple2.of("1", "\tKEY值字段长度\t->\t默认值：'1' \\ 如果是'-1' 会给一个固定值 \\ 如果是 -2 会故意产生数据偏移"));
            put("--valuescope", Tuple2.of("10", "\tVALUE值范围（随机值）\t->\t默认值：'10' \\ 如果给 -1 所有的value都会变成 1 \\ 如果给 -2 就是累加"));
            put("--time", Tuple2.of("no", "\t是否显时间戳\t->\t默认值：'no' \\ 可选：'yes'"));
            put("--source", Tuple2.of("socket", "\t传出类型\t->\t默认值:'socket' \\ 可选：'Kafka' 注意Kafka后面要添加:topic 案例 kafka:topicname"));
            put("--separator", Tuple2.of(":", "\t分割字段\t\t->\t默认值:':' \\ 只有类型是kv的时候生效"));
            put("--port", Tuple2.of(String.valueOf(SocketProp.port), "\t端口号\t\t->\t默认值:'" + SocketProp.port + "'"));
            put("--sum", Tuple2.of("300", "\t循环次数\t\t->\t默认值:':300' \\ 可选：'-1' 无穷"));
            put("--masterkey", Tuple2.of("", "\t特殊key，偏移key和单一自定义key\t\t->\t默认值:'null'"));
            put("--sleep", Tuple2.of("500", "\t休眠时间 毫秒\t\t->\t默认值 : '500'"));
            put("--fluctuation",Tuple2.of("no" ,"\t模拟波动，默认关闭\t参数填写 波动时间(毫秒)/波动间隔(int) 案例: 5000/5"));
        }
    };

    public static void main(String[] args) throws Exception{
        String separator,time,type,source;
        int KeyLength,ValueScope,port;
        // 初始化
        initiate(args);
        // 传入变量
        type = MetaDataMap.get("--type").f0;
        KeyLength = Integer.parseInt(MetaDataMap.get("--keylength").f0);
        ValueScope = Integer.parseInt(MetaDataMap.get("--valuescope").f0);
        time = MetaDataMap.get("--time").f0;
        separator = MetaDataMap.get("--separator").f0;
        port = Integer.parseInt(MetaDataMap.get("--port").f0);
        source = MetaDataMap.get("--source").f0;
        sum = Integer.parseInt(MetaDataMap.get("--sum").f0);
        masterKey = MetaDataMap.get("--masterkey").f0;
        sleepTime = Long.parseLong(MetaDataMap.get("--sleep").f0);
        fluctuation = MetaDataMap.get("--fluctuation").f0;

        if (source.equals("socket"))
            SocketMSG(port, type, KeyLength, ValueScope, time, separator);
        if (source.contains("kafka")){
            kafkaMSG(source, type, KeyLength, ValueScope, time, separator);
        }

    }

    private static void kafkaMSG(String source, String type, int KeyLength, int ValueScope, String time, String separator) {
        while (cyclic()) {
            String topic = source.split("kafka:")[1];
            count++;
            String str;
            if (type.equals("input"))
                str = input();
            else {
                str = socketStream(KeyLength, ValueScope, time, type, separator);
            }
            KafkaUtil.sendMsg(topic,str);
            String k = "k";
            if (!masterKey.isEmpty())
                k = masterKey;
            if (KeyLength == -2)
                for (int i = 0; i < KeyLength+5; i++) {
                    KafkaUtil.sendMsg(topic,setKeyValue(ValueScope, time, type, separator,k));
                }
            System.out.println(str);
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void SocketMSG(int port, String type, int KeyLength, int ValueScope, String time, String separator) {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("\n\t=====>服务启动，等待连接");
            Socket socket = serverSocket.accept();
            System.out.println("连接成功，来自：" + socket.getRemoteSocketAddress());
            pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            int j = 0;

//            while (j < 200) {
            while (cyclic()) {
                count++;
                String str;
                if (type.equals("input"))
                    str = input();
                else {
                    str = socketStream(KeyLength, ValueScope, time, type, separator);
                }
                pw.println(str);
                pw.flush();
                String k = "k";
                if (!masterKey.isEmpty())
                    k = masterKey;

                if (KeyLength == -2)
                    for (int i = 0; i < KeyLength+5; i++) {
                        pw.println(setKeyValue(ValueScope, time, type, separator,k));
                        pw.flush();
                    }
                System.out.println(str);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                pw.close();
                serverSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean cyclic(){
        if (sum > -1)
            return count < sum;
        return true;
    }

    /**
     * 初始化
     * @param args 外部参数
     */
    private static void initiate(String[] args) {
        ArrayList<String> MetaData = new ArrayList<>();
        if (args.length >=1 && args[0].equalsIgnoreCase("--help")){
            help();
            System.exit(0);
        }
        if (args.length < 1)
            help();
        // 解析参数
        inParameter(args, MetaData);
    }

    /**
     * 入参数方法
     * @param args 参数
     * @param MetaData  不用管
     */
    private static void inParameter(String[] args, ArrayList<String> MetaData) {
        for (int i = 0, a = 0; i < args.length; i++) {
            if (args[i].contains("--")){
                if (i+1< args.length && !args[i+1].contains("--")){
                    String argument = args[i].toLowerCase();
                    String value = args[i + 1];
                    for (String key : MetaDataMap.keySet()) {
                        if (key.contains(argument)){
                            MetaDataMap.put(key,Tuple2.of(value,MetaDataMap.get(key).f1));
                            System.out.println(key + ":" + value);
                        }
                    }
                    i++;
                }else {
                    System.out.println("ERROR\t参数有误\\已经去除 ==> "+ args[i]);
                }
            }else {
                System.out.println(" ==>\t"+ args[a]);
                MetaData.add(args[a]);
                a++;
            }
        }
        String[] Metas =  MetaDataMap.keySet().toArray(new String[0]);
        for (int i = 0; i < Math.min(Metas.length, MetaData.size()); i++) {
            MetaDataMap.put(Metas[i],Tuple2.of(MetaData.get(i),MetaDataMap.get(Metas[i]).f1));
        }
    }

    /**
     * 帮助文档
     */
    private static void help() {
        System.out.println("目前可输入参数:\\");
        for (String key : MetaDataMap.keySet()) {
            System.out.println(key+"\n"+MetaDataMap.get(key).f1);
        }
    }

    /**
     * 输入
     * @return 输出字符串
     */
    private static String input() {
        String str;
        Scanner scanner = new Scanner(System.in);
        str = scanner.next();
        return str;
    }

    /**
     * 获取每个 socket 的值
     * @param KeyLength key长度，为测试为字母，随机
     * @param ValueScope value 的范围 0 - x
     * @param time  是否有时间 yes / no
     * @param type  类型 kv / json
     * @param separator 分隔符 type是kv的时候生效
     * @return  socket 发送内容
     */
    private static String socketStream(int KeyLength, int ValueScope, String time, String type, String separator) {
        // 获取kv
        String key = "k";
        if (!masterKey.isEmpty()){
            key = masterKey;
        }
        if (KeyLength > -1)
            key = RandomStringUtils.randomAlphabetic(KeyLength).toLowerCase(Locale.ROOT);
        String str = setKeyValue(ValueScope, time, type, separator, key);
        return str;
    }

    private static String setKeyValue(int ValueScope, String time, String type, String separator, String key) {
        String value = "1";
        if (ValueScope == -2){
            value = String.valueOf(count);

        }
        if (ValueScope > -1)
            value = String.valueOf(new Random().nextInt(ValueScope));

        String str = "";
        long createTime;
        // 是否需要时间
        if (time.equalsIgnoreCase("yes")){
            if (!fluctuation.equals("no")){
                int offset = Integer.parseInt(fluctuation.split("/")[1]);
                long offsetTime = Long.parseLong(fluctuation.split("/")[0]);
                if(count % offset == 0)
                    createTime = System.currentTimeMillis() - ThreadLocalRandom.current().nextLong(offsetTime/4, offsetTime);
                else
                    createTime = System.currentTimeMillis();
            }else {
                createTime = System.currentTimeMillis();
            }
        }else{
            createTime = -1L;
        }
        // 判断类型
        if (type.equalsIgnoreCase("kv")){
            str = kv(key, separator, value, createTime);
        } else if (type.equalsIgnoreCase("json")) {
            str = jsonKv(key, value, createTime);
        }
        return str;
    }

    /**
     * json数据简单处理
     * @param key  key
     * @param value value
     * @param createTime 时间戳 -1表示没有
     * @return {key:"",value:""}
     */
    private static String jsonKv(String key, String value, Long createTime) {
        String str;
        HashMap<String,Object> map = new HashMap<>();
        map.put("key", key);
        map.put("value", value);
        if (createTime != -1)
            map.put("createTime", createTime);
        str = simpleObjToJson(map);
        return str;
    }

    /**
     * kv数据简单处理
     * @param key   key
     * @param separator 分隔符
     * @param value value
     * @param createTime 时间戳，-1表示没有
     * @return k:v
     */
    private static String kv(String key, String separator, String value, Long createTime) {
        String str;
        str = key + separator + value;
        if (createTime != -1)
            str += separator + createTime;
        return str;
    }

    /**
     * object 转 json
     * @param obj object
     * @return  String
     */
    private static String simpleObjToJson(Object obj) {
        if (Objects.isNull(obj)) return "";
        try {
            return JSON.toJSONString(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
}
