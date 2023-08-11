package com.jarninlee.mydb.dataManager.dataitem;

import com.jarninlee.mydb.common.SubArray;
import com.jarninlee.mydb.dataManager.DataManager;
import com.jarninlee.mydb.dataManager.DataManagerImpl;
import com.jarninlee.mydb.dataManager.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem{
    static final int OF_VALID = 0;
    static final int OF_SIZE = OF_VALID + 1;
    static final int OF_DATA = OF_SIZE + 2;

    private SubArray subArray;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dataManager;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray subArray, byte[] oldRaw, DataManagerImpl dataManager, long uid, Page page) {
        this.subArray = subArray;
        this.oldRaw = oldRaw;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
        this.dataManager = dataManager;
        this.uid = uid;
        this.page = page;
    }

    public boolean isValid(){
        return subArray.raw[subArray.start + OF_VALID] == (byte) 0; // ? whats mean
    }

    @Override
    public SubArray data() {
        return new SubArray(subArray.raw, subArray.start + OF_DATA, subArray.end);
    }

    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(subArray.raw,subArray.start,oldRaw,0,oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw,0,subArray.raw,subArray.start,oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dataManager.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dataManager.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }
}
