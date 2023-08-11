package com.jarninlee.mydb.dataManager;

import com.jarninlee.mydb.common.AbstractCache;
import com.jarninlee.mydb.dataManager.dataitem.DataItem;
import com.jarninlee.mydb.dataManager.dataitem.DataItemImpl;
import com.jarninlee.mydb.dataManager.logger.Logger;
import com.jarninlee.mydb.dataManager.page.Page;
import com.jarninlee.mydb.dataManager.pageIndex.PageIndex;
import com.jarninlee.mydb.dataManager.pcache.PageCache;
import com.jarninlee.mydb.transactionManager.TransactionManager;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {
    TransactionManager transactionManager;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(TransactionManager transactionManager, PageCache pageCache, Logger logger) {
        super(0);
        this.transactionManager = transactionManager;
        this.pageCache = pageCache;
        this.logger = logger;
    }

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

    @Override
    protected DataItem getForCache(long key) throws Exception {
        return null;
    }

    @Override
    protected void releaseForCache(DataItem obj) {

    }

    public void logDataItem(long xid, DataItem di){

    }

    public void releaseDataItem(DataItem di){

    }

    public void initPageOne(){

    }

    public boolean logCheckPageOne(){
        return true;
    }
}
