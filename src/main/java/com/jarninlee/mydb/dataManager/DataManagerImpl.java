package com.jarninlee.mydb.dataManager;

import com.jarninlee.mydb.common.AbstractCache;
import com.jarninlee.mydb.dataManager.dataitem.DataItem;
import com.jarninlee.mydb.dataManager.dataitem.DataItemImpl;
import com.jarninlee.mydb.dataManager.logger.Logger;
import com.jarninlee.mydb.dataManager.page.Page;
import com.jarninlee.mydb.dataManager.page.PageCommon;
import com.jarninlee.mydb.dataManager.page.PageOne;
import com.jarninlee.mydb.dataManager.pageIndex.PageIndex;
import com.jarninlee.mydb.dataManager.pageIndex.PageInfo;
import com.jarninlee.mydb.dataManager.pcache.PageCache;
import com.jarninlee.mydb.transactionManager.TransactionManager;
import com.jarninlee.mydb.utils.Panic;
import com.jarninlee.mydb.utils.Types;

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

        DataItemImpl dataItem = null;
        try {
            dataItem = (DataItemImpl) super.get(uid);
        } catch (Exception e) {
            throw e;
        }
        if(!dataItem.isValid()){
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    //返回值为：pageNum << 32 | offset
    @Override
    public long insert(long xid, byte[] data)  throws Exception{
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageCommon.MAX_FREE_SPACE){
            throw new RuntimeException("data is too long!");
        }

        PageInfo pageInfo = null;

        for (int i = 0; i < 5; i++) {
            pageInfo = pageIndex.select(raw.length); // 需要页面的空闲长度至少为raw.length
            if(pageInfo != null){
                break;
            }else {
                //如果pageIndex中没有缓存合适的页面，那就新建 空闲页面 并添加至索引缓存中
                int newPageNum = pageCache.newPage(PageCommon.initRaw());
                pageIndex.add(newPageNum,PageCommon.MAX_FREE_SPACE);
            }
        }

        if(pageInfo == null){
            throw new RuntimeException("Database is busy");
        }

        Page page = null;
        int freeSpace = 0;

        try {
            try {
                page = pageCache.getPage(pageInfo.pgno);
            } catch (Exception e) {
                throw e;
            }
            //先落日志  在进行磁盘操作（写入操作在 release()中）
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);

            short offset = PageCommon.insert(page, raw);

            //这里进行 磁盘真正写入
            page.release();
            return Types.addressToUid(pageInfo.pgno, offset);
        } finally {
            //最后将页面在存入pageIndex缓存  这样实现了 单独写（取页面直接 再缓存中删除页面  处理完在根据剩余空闲容量回填入缓存）
            if(page != null){
                pageIndex.add(pageInfo.pgno, PageCommon.getFreeSpace(page));
            }else {
                pageIndex.add(pageInfo.pgno,freeSpace); //TODO  什么含义
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        pageCache.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16)  - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page page = null;
        try {
            page = pageCache.getPage(pgno);
        } catch (Exception e) {
            throw e;
        }
        return DataItem.parseDataItem(page,offset,this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    //为xid生成log日志
    public void logDataItem(long xid, DataItem di){
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di){
        super.release(di.getUid());
    }

    //创建文件时初始化pageOne
    public void initPageOne(){
        int pageNum = pageCache.newPage(PageOne.initRaw());
        assert pageNum == 1;
        try {
            pageOne = pageCache.getPage(pageNum);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageOne);
    }

    //在打开已有文件时读入PageOne验证
    public boolean loadCheckPageOne(){
        try {
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    //初始化pageIndex
    void fillPageIndex(){
        int pageNumber = pageCache.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(pageNumber,PageCommon.getFreeSpace(pg));
            assert pg != null;
            pg.release();
        }
    }
}
