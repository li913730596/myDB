package com.jarninlee.mydb.dataManager;

import com.jarninlee.mydb.dataManager.dataitem.DataItem;

public class DataManagerImpl implements DataManager {
    @Override
    public DataItem read(long uid) throws Exception {
        //TODO: auto generated method stub

        return null;
    }

    @Override
    public long insert(long xid, byte[] data) {
        //TODO: auto generated method stub
        return 0;
    }

    @Override
    public void close() {
        //TODO: auto generated method stub
    }

    public void logDataItem(long xid, DataItem di){

    }

    public void releaseDataItem(DataItem di){

    }
}
