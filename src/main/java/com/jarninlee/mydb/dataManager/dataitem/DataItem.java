package com.jarninlee.mydb.dataManager.dataitem;

import com.google.common.primitives.Bytes;
import com.jarninlee.mydb.common.Parser;
import com.jarninlee.mydb.common.SubArray;
import com.jarninlee.mydb.dataManager.DataManagerImpl;
import com.jarninlee.mydb.dataManager.page.Page;

public interface DataItem {
    SubArray data();
    void before();
    void unBefore();
    void after(long xid);
    void release();
    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    public static byte[] wrapDataItemRaw(byte[] raw){
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid,size,raw);
    }
    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm){
        byte[] raw = pg.getData();
        return null;
    }
}
