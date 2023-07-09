package com.jarninlee.mydb.common;

import java.nio.ByteBuffer;

public class Parser{
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
