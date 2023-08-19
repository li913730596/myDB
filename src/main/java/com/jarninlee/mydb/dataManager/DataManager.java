package com.jarninlee.mydb.dataManager;

import com.jarninlee.mydb.dataManager.dataitem.DataItem;
import com.jarninlee.mydb.dataManager.logger.Logger;
import com.jarninlee.mydb.dataManager.page.PageOne;
import com.jarninlee.mydb.dataManager.pcache.PageCache;
import com.jarninlee.mydb.transactionManager.TransactionManager;
import com.jarninlee.mydb.transactionManager.TransactionManagerImpl;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();
    public static DataManager create(String path, long mem, TransactionManager tm){
        PageCache pageCache = PageCache.create(path, mem);
        Logger logger = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(tm, pageCache, logger);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm){
        PageCache pageCache = PageCache.open(path, mem);
        Logger logger = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(tm, pageCache, logger);
        if(!dm.loadCheckPageOne()){
            Recover.recover(tm,pageCache,logger);
        }
        dm.fillPageIndex();
        PageOne.checkVc(dm.pageOne);
        dm.pageCache.flushPage(dm.pageOne);

        return dm;
    }
}
