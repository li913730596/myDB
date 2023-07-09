package com.jarninlee.mydb;

import com.jarninlee.mydb.common.MockCache;
import com.jarninlee.mydb.utils.Panic;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class CacheTest {

    private CountDownLatch cdl;
    private MockCache cache;

    @Test
    public void testCache() {
        cache = new MockCache();
        cdl = new CountDownLatch(10);
        for(int i = 0; i < 10; i ++) {
            Runnable r = () -> work();
            new Thread(r).start();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void work() {
        for(int i = 0; i < 10; i++) {
            long uid = new Random(System.nanoTime()).nextInt(10);
            long h = 0;
            try {
                h = cache.get(uid);
            } catch (Exception e) {
                Panic.panic(e);
            }
            assert h == uid;
        }
        cdl.countDown();
    }
}