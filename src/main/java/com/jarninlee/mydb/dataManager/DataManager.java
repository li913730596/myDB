package com.jarninlee.mydb.dataManager;

import com.jarninlee.mydb.dataManager.dataitem.DataItem;
import com.jarninlee.mydb.dataManager.logger.Logger;
import com.jarninlee.mydb.dataManager.pcache.PageCache;
import com.jarninlee.mydb.dataManager.pcache.PageCacheImpl;
import com.jarninlee.mydb.transactionManager.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data);
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
        if(!dm.logCheckPageOne()){
            
        }
        return dm;
    }
}
