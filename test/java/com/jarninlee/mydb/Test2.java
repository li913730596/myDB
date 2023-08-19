package com.jarninlee.mydb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
public class Test2 {
    @Test
    public void test2() {
//        String p = "C:\\Users\\JarNinLee\\Desktop\\Java\\myDB\\src\\main\\resources";
//        File file = new File(p + "\\pcacher_simple_test0.db");
//        Path filePath = file.toPath();
//
//        assert file.delete();
//        System.out.println("success");
        long x = 33;
        long y = 22;
        long uid = x << 32 | y;

        short a = (short)(uid & (1L << 16) - 1);
        System.out.println(a);
        uid >>>= 32;

        int b = (int)(uid & (1L << 32) - 1);
        System.out.println(b);
    }
}
