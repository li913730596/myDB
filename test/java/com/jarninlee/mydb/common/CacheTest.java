package com.jarninlee.mydb.common;

import com.jarninlee.mydb.common.AbstractCache;
import com.jarninlee.mydb.common.MockCache;
import com.jarninlee.mydb.utils.Panic;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class CacheTest {

    private CountDownLatch cdl;
    private AbstractCache<Long> cache;

    @Test
    public void testCache() {
        cache = new MockCache();
        cdl = new CountDownLatch(100);
        for(int i = 0; i < 100; i ++) {
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
            long uid = new Random(System.nanoTime()).nextInt();
            long h = 0;
            try {
                h = cache.get(uid);
            } catch (Exception e) {
                if(e.getMessage().equals("Cache is full !!!")) continue;
                Panic.panic(e);
            }
            assert h == uid;
            cache.release(h);
        }
        cdl.countDown();
    }
}