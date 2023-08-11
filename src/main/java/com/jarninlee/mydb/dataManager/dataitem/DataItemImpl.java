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

    //DataItem的整体就在subArray中的raw存储
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
        return subArray.raw[subArray.start + OF_VALID] == (byte) 0; // dataitem中的raw里面的valid字段 == 0
    }

    //dataItem中的raw中的字段  偏移量为3
    @Override
    public SubArray data() {
        return new SubArray(subArray.raw, subArray.start + OF_DATA, subArray.end);
    }

    //操作dataItem之前必须先将raw存入OldRaw中进行备份
    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(subArray.raw, subArray.start, oldRaw,0,oldRaw.length); //TODO:为何是oldRaw.length
    }

    //撤销修改之前需要将oldRaw先存回去
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw,0,subArray.raw,subArray.start,oldRaw.length);
        wLock.unlock();
    }

    //修改完之后调用after  将修改操作记录成日志
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

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return subArray;
    }
}
