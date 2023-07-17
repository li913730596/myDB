package com.jarninlee.mydb.common;

import java.nio.ByteBuffer;


public class Parser{

    public static byte[] int2Byte(int value){
       return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    public static int parseInt(byte[] buf){
        return ByteBuffer.wrap(buf,0,4).getInt();
    }

    //将字节数组前八个字节 解析成一个long
    public static long parseLong(byte[] buf){
        ByteBuffer buffer = ByteBuffer.wrap(buf,0,8);
        return buffer.getLong();
    }

    //将 long 转化成对应的 byte[]类型
    public static byte[] long2byte(long value){
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
