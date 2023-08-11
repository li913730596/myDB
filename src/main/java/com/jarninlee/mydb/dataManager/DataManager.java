package com.jarninlee.mydb.dataManager;

import com.jarninlee.mydb.dataManager.dataitem.DataItem;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data);
    void close();
}
