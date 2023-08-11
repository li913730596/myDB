package com.jarninlee.mydb.dataManager.pcache;

import com.jarninlee.mydb.dataManager.page.Page;
import com.jarninlee.mydb.utils.Panic;
import com.jarninlee.mydb.utils.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class PageCacheTest {

    String p = "src/main/resources";

    @Test
    public void testPageCache() throws Exception {
        PageCache pc = PageCache.create(p + "/pcacher_simple_test0.db", PageCache.PAGE_SIZE * 50);
        for (int i = 0; i < 100; i++) {
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            tmp[0] = (byte) i;
            int pgno = pc.newPage(tmp);
            Page pg = pc.getPage(pgno);
            pg.setDirty(true);
            pg.release();
        }
        pc.close();

        pc = PageCache.open(p + "/pcacher_simple_test0.db", PageCache.PAGE_SIZE * 50);
        for (int i = 1; i <= 100; i++) {
            Page pg = pc.getPage(i);
            assert pg.getData()[0] == (byte) i - 1;
            pg.release();
        }
        pc.close();

        assert new File(p + "/pcacher_simple_test0.db").delete();
    }

    private PageCache pc1;
    private CountDownLatch cdl1;
    private AtomicInteger noPages1;

    @Test
    public void testPageCacheMultiSimple() {
        pc1 = PageCache.create(p + "/pcacher_simple_test0.db", PageCache.PAGE_SIZE * 50);
        cdl1 = new CountDownLatch(20);
        noPages1 = new AtomicInteger(0);
        for (int i = 0; i < 20; i++) {
            int id = i;
            Runnable r = () -> worker1(id);
            new Thread(r).start();
        }
        try {
            cdl1.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        File file = new File(p + "/pcacher_simple_test0.db");
//        if (file.canWrite() && file.canExecute()) System.out.println("YES");
        //TODO 这里无法删除，很有可能是有其他进程访问了文件
        assert file.delete();
    }

    private void worker1(int id) {
        for (int i = 0; i < 80; i++) {
            int op = new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % 20;
            if (op == 0) {
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                int pgno = pc1.newPage(data);
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                noPages1.incrementAndGet();
                pg.release();
            } else if (op < 20) {
                int mod = noPages1.intValue();
                if (mod == 0) {
                    continue;
                }
                int pgno = Math.abs(new Random(System.currentTimeMillis()).nextInt()) % mod + 1;
                Page pg = null;
                try {
                    pg = pc1.getPage(pgno);
//                    log.info("{} {}",Thread.currentThread(),pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                pg.release();
            }
        }
        cdl1.countDown();
    }


    private PageCache pc2, mpc;
    private CountDownLatch cdl2;
    private AtomicInteger noPages2;
    private Lock lockNew;

    @Test
    public void testPageCacheMulti() throws InterruptedException {
        pc2 = PageCache.create(p + "/pcacher_simple_test0.db", PageCache.PAGE_SIZE * 10);
        mpc = new MockPageCache();
        lockNew = new ReentrantLock();

        cdl2 = new CountDownLatch(30);
        noPages2 = new AtomicInteger(0);

        for (int i = 0; i < 30; i++) {
            int id = i;
            Runnable r = () -> worker2(id);
            new Thread(r).run();
        }
        cdl2.await();

        assert new File(p + "/pcacher_simple_test0.db").delete();
    }

    private void worker2(int id) {
        for (int i = 0; i < 1000; i++) {
            int op = new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % 20;
            if (op == 0) {
                // new page
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                lockNew.lock();
                int pgno = pc2.newPage(data);
                int mpgno = mpc.newPage(data);
                assert pgno == mpgno;
                lockNew.unlock();
                noPages2.incrementAndGet();
            } else if (op < 10) {
                // check
                int mod = noPages2.intValue();
                if (mod == 0) continue;
                int pgno = Math.abs(new Random(System.currentTimeMillis()).nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                pg.lock();
                assert Arrays.equals(mpg.getData(), pg.getData());
                pg.unlock();
                pg.release();
            } else {
                // update
                int mod = noPages2.intValue();
                if (mod == 0) continue;
                int pgno = Math.abs(new Random(System.currentTimeMillis()).nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc.getPage(pgno);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                byte[] newData = RandomUtil.randomBytes(PageCache.PAGE_SIZE);

                pg.lock();
                mpg.setDirty(true);
                for (int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    mpg.getData()[j] = newData[j];
                }
                pg.setDirty(true);
                for (int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    pg.getData()[j] = newData[j];
                }
                pg.unlock();
                pg.release();
            }
        }
        cdl2.countDown();
    }
}