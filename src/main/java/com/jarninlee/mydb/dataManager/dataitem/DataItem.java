package com.jarninlee.mydb.dataManager.dataitem;

import com.google.common.primitives.Bytes;
import com.jarninlee.mydb.common.Parser;
import com.jarninlee.mydb.common.SubArray;
import com.jarninlee.mydb.dataManager.DataManagerImpl;
import com.jarninlee.mydb.dataManager.page.Page;
import com.jarninlee.mydb.utils.Types;

import java.lang.reflect.Type;
import java.util.Arrays;

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

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw){
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid,size,raw);
    }
    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm){
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw,offset,offset + length),new byte[length],dm,uid,pg);
    }

    public static void SetDataItemRawInvalid(byte[] raw){
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
