package com.an.job.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.an.job.liveStreaming.pojo.DataBean;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class JsonUtil<T> {
    public List<T> readListFile(String filePath, Class<T> clazz) {
        File file = new File(filePath);
        return readListFile(file,clazz);
    }
    public List<T> readListFile(File file, Class<T> clazz) {
        try {
            String content = new String(Files.readAllBytes(file.toPath()));
            JSONArray jsonArray = JSON.parseArray(content);
            return jsonArray.toJavaList(clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
