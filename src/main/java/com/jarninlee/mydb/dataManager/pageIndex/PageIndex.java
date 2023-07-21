package com.jarninlee.mydb.dataManager.pageIndex;

import com.jarninlee.mydb.dataManager.pcache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    //将1页划分为40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] list;

    public PageIndex() {
        lock = new ReentrantLock();
        list = new List[INTERVALS_NO + 1];
        for (int i = 0; i < list.length; i++) {
            list[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            list[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }


    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) number++;  //页数从 1 到 40

            while (number <= INTERVALS_NO) {
                if (list[number].size() == 0) {
                    number++;
                    continue;
                }
                return list[number].remove(0);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }
}
