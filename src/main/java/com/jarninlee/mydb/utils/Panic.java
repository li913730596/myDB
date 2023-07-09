package com.jarninlee.mydb.utils;

//打印错误的工具类
public class Panic {
    public static void panic(Exception err){
        err.printStackTrace();
        //终止当前正在执行的 Java 虚拟机（JVM）进程
        System.exit(1);
    }
}
