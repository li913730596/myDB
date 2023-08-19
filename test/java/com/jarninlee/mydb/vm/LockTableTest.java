package com.jarninlee.mydb.vm;

import com.jarninlee.mydb.utils.Panic;
import com.jarninlee.mydb.versionmanager.LockTable;
import org.junit.Test;

import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertThrows;

public class LockTableTest {

    @Test
    public void testLockTable() {
        LockTable lt = LockTable.newLockTable();
        try {
            lt.add(1, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lt.add(2, 2);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lt.add(2, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }

        assertThrows(RuntimeException.class, ()->lt.add(1, 2));
    }

    @Test
    public void testLockTable2() {
        LockTable lt = LockTable.newLockTable();
        for(long i = 1; i <= 100; i ++) {
            try {
                Lock o = lt.add(i, i);
                if(o != null) {
                    Runnable r = () -> {
                        o.lock();
                        o.unlock();
                    };
                    new Thread(r).run();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        for(long i = 1; i <= 99; i ++) {
            try {
                Lock o = lt.add(i, i+1);
                if(o != null) {
                    Runnable r = () -> {
                        o.lock();
                        o.unlock();
                    };
                    new Thread(r).run();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        assertThrows(RuntimeException.class, ()->lt.add(100, 1));
        lt.remove(23);

        try {
            lt.add(100, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }
}
