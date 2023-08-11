package com.jarninlee.mydb.dataManager.page;

import com.jarninlee.mydb.transactionManager.TransactionManager;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
@Slf4j
public class Test1 {

    int noWorks = 3000;
    int noWorkers = 50;
    int tranCount = 0;
    Lock lock = new ReentrantLock();
    TransactionManager transactionManager;
    Map<Long, Byte> TranMap;
    CountDownLatch cdl;
    AtomicInteger cnt = new AtomicInteger(1);

    @Test
    public void testMutilThread(){
        String path = "src/main/resources/tranmger_test.xid";
        Path path1 = Path.of(path);
        if(Files.exists(path1)){
            try {
                Files.delete(path1);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        transactionManager = TransactionManager.create(path);
        cdl = new CountDownLatch(noWorkers);
        TranMap = new ConcurrentHashMap<Long, Byte>();
        for (int i = 0; i < noWorkers; i ++){
            Runnable r = () -> worker();
            new Thread(r,"Thread: " + i).start();
        }

        try {
            cdl.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public void worker(){
//        log.info("{}  {}",Thread.currentThread(),cnt.getAndIncrement());
        boolean inTrans = false;
        long transXID = 0;

        for (int i = 0; i < noWorks; i++) {
            int op = new Random(System.nanoTime()).nextInt(6);
//            log.info("{}   {}  {}",Thread.currentThread(),i,op);
            if(op == 0){
                lock.lock();
//                log.info("{}",Thread.currentThread());
                if(inTrans == false){
                    long xid = transactionManager.begin();
                    TranMap.put(xid, (byte) 0);
                    tranCount ++;
                    transXID = xid;
                    inTrans = true;
                }else{
                    int status = (new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE % 2)) + 1;
//                    log.info("{}",status);
                    switch (status){
                        case 1:
                            transactionManager.commit(transXID);
                            break;
                        case 2:
                            transactionManager.abort(transXID);
                            break;
                    }
                    TranMap.put(transXID,(byte)status);
//                    inTrans = false;
                }
                lock.unlock();
            }else {
                lock.lock();
                if(tranCount > 0) {
                    long xid = (new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % tranCount) + 1;
                    Byte status = TranMap.get(xid);
                    boolean ok = false;
                    switch (status){
                        case 0:
                            ok = transactionManager.isActive(xid);
                            break;
                        case 1:
                            ok = transactionManager.isCommited(xid);
                            break;
                        case 2:
                            ok = transactionManager.isAbort(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
//        log.info("{}", Thread.currentThread());
        cdl.countDown();
    }
}

//import com.jarninlee.mydb.transactionManager.TransactionManager;
//import org.junit.Test;
//
//import java.util.Map;
//import java.util.Random;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.locks.Lock;
//import java.util.concurrent.locks.ReentrantLock;
//
//public class com.jarninlee.mydb.dataManager.page.Test1{
//    private int transCnt = 0;
//    private int noWorkers = 50;
//    private int noWorks = 3000;
//    private Lock lock = new ReentrantLock();
//    private TransactionManager tmger;
//    private Map<Long, Byte> transMap;
//    private CountDownLatch cdl;
//
//    @Test
//    public void testMultiThread() {
//        transMap = new ConcurrentHashMap<>();
//        cdl = new CountDownLatch(noWorkers);
//        for(int i = 0; i < noWorkers; i ++) {
//            Runnable r = () -> worker();
//            new Thread(r).run();
//        }
//        try {
//            cdl.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void worker() {
//        boolean inTrans = false;
//        long transXID = 0;
//        for(int i = 0; i < noWorks; i ++) {
//            int op = new Random(System.nanoTime()).nextInt(6);
//            if(op == 0) {
//                lock.lock();
//                if(inTrans == false) {
//                    long xid = tmger.begin();
//                    transMap.put(xid, (byte)0);
//                    transCnt ++;
//                    transXID = xid;
//                    inTrans = true;
//                } else {
//                    int status = (new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % 2) + 1;
//                    switch(status) {
//                        case 1:
//                            tmger.commit(transXID);
//                            break;
//                        case 2:
//                            tmger.abort(transXID);
//                            break;
//                    }
//                    transMap.put(transXID, (byte)status);
//                    inTrans = false;
//                }
//                lock.unlock();
//            } else {
//                lock.lock();
//                if(transCnt > 0) {
//                    long xid = (long)((new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % transCnt) + 1);
//                    byte status = transMap.get(xid);
//                    boolean ok = false;
//                    switch (status) {
//                        case 0:
//                            ok = tmger.isActive(xid);
//                            break;
//                        case 1:
//                            ok = tmger.isCommited(xid);
//                            break;
//                        case 2:
//                            ok = tmger.isAbort(xid);
//                            break;
//                    }
//                    assert ok;
//                }
//                lock.unlock();
//            }
//        }
//        cdl.countDown();
//    }
//}