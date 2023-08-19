package com.jarninlee.mydb.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtil {
    public static byte[] randomBytes(int length){
        SecureRandom random = new SecureRandom();
        byte[] buf = new byte[length];
        random.nextBytes(buf);
        return buf;
    }
}