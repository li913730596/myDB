package com.jarninlee.mydb.utils;

import java.util.Random;

public class RandomUtil {
    public static byte[] randomBytes(int length){
        Random random = new Random(System.nanoTime());
        byte[] buf = new byte[length];
        for (int i = 0; i < length; i++) {
            int t = random.nextInt(Integer.MAX_VALUE) % 62;
            if(t < 26){
                buf[i] = (byte) ('a' + t);
            }else if (t < 52){
                buf[i] = (byte) ('A' + t - 26);
            }else {
                buf[i] = (byte) ('0' + t - 52);
            }
        }
        return buf;
    }
}
