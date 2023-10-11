package com.an.job.util;


import com.an.job.util.enumerate.blue;
import com.an.job.util.enumerate.green;
import com.an.job.util.enumerate.red;
import com.an.job.util.enumerate.yellow;

public class log {
    public static String primary(String string){
        return blue.start.getValue()+string+blue.end.getValue();
    }
    public static String warning(String string){
        return yellow.start.getValue()+string+yellow.end.getValue();
    }
    public static String success(String string){
        return green.start.getValue()+string+green.end.getValue();
    }
    public static String error(String string){
        return red.start.getValue()+string+red.end.getValue();
    }
}
