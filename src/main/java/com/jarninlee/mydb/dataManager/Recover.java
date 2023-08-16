package com.jarninlee.mydb.dataManager;

import com.google.common.primitives.Bytes;
import com.jarninlee.mydb.common.Parser;
import com.jarninlee.mydb.common.SubArray;
import com.jarninlee.mydb.dataManager.dataitem.DataItem;
import com.jarninlee.mydb.dataManager.logger.Logger;
import com.jarninlee.mydb.dataManager.page.Page;
import com.jarninlee.mydb.dataManager.page.PageCommon;
import com.jarninlee.mydb.dataManager.pcache.PageCache;
import com.jarninlee.mydb.transactionManager.TransactionManager;
import com.jarninlee.mydb.transactionManager.TransactionManagerImpl;

import java.util.*;

public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager transactionManager, PageCache pageCache, Logger logger){
        System.out.println("Recoving.....");

        logger.rewind();
        int maxPgno = 0;
        while(true){
            byte[] log = logger.next();
            if(log == null) break;
            int pgno;
            if (isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                pgno = insertLogInfo.pgno;
            }else{
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                pgno = updateLogInfo.pgno;
            }
            if(pgno > maxPgno){
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0){   //说明日志文件为空
            maxPgno = 1; //TODO 为什么要等于一， 此时应当没有日志才对   因为页号从一开始
        }
        pageCache.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " Pages.");

        redoTranscations(transactionManager,logger,pageCache);
        System.out.println("Redo transactions Over");

        undoTranscations(transactionManager,logger,pageCache);
        System.out.println("Undo transactions Over");

        System.out.println("Recovery Over");
    }

    private static void redoTranscations(TransactionManager transactionManager, Logger logger, PageCache pageCache){
        logger.rewind();
        while(true){
            byte[] log = logger.next();
            if(log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid  = insertLogInfo.xid;
                if(!transactionManager.isActive(xid)){
                    doInsertLog(pageCache,log,REDO);
                }
            }else{
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if(!transactionManager.isActive(xid)){
                    doUpdateLog(pageCache,log,REDO);
                }
            }
        }
    }

    private static void undoTranscations(TransactionManager transactionManager, Logger logger, PageCache pageCache){
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        while(true){
            byte[] log = logger.next();
            if(log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;
                if(transactionManager.isActive(xid)){
                    if(!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }else{
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if(transactionManager.isActive(xid)){
                    if(!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        //对所有Active的log进行 倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i -- ){
                byte[] log = logs.get(i);
                if(isInsertLog(log)){
                    doInsertLog(pageCache,log,UNDO);
                }else{
                    doUpdateLog(pageCache,log,UNDO);
                }
            }
            transactionManager.abort(entry.getKey()); // 回滚xid对应的事务
        }
    }


    private static boolean isInsertLog(byte[] log){
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LOGType] [XID] [UID] [OldRaw] [NewRaw]
    //     1       8     8
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    //创建updateLog 格式如上
    public static byte[] updateLog(long xid, DataItem dataItem){
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2byte(xid);
        byte[] uidRaw = Parser.long2byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw,raw.start,raw.end);
        return Bytes.concat(logType,xidRaw,uidRaw,oldRaw,newRaw);
    }

    //解析updateLog
    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log,OF_UPDATE_UID,OF_UPDATE_RAW));
        updateLogInfo.offset = (short) (uid & (1L << 16) - 1);
        uid >>>= 32;
        updateLogInfo.pgno = (int) (uid & (1L << 32) - 1);
        int len = (log.length - OF_UPDATE_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW, OF_UPDATE_RAW + len);
        updateLogInfo.newRaw = Arrays.copyOfRange(log,OF_UPDATE_RAW + len, OF_UPDATE_RAW + len * 2);
        return updateLogInfo;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag){
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO){
            UpdateLogInfo updateLog = parseUpdateLog(log);
            pgno = updateLog.pgno;
            offset = updateLog.offset;
            raw = updateLog.newRaw;
        }else{
            UpdateLogInfo updateLog = parseUpdateLog(log);
            pgno = updateLog.pgno;
            offset = updateLog.offset;
            raw = updateLog.oldRaw;
        }

        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            PageCommon.recoverUpdate(pg,raw,offset);
        } finally {
            pg.release();
        }
    }

    // [LOGType] [XID] [Pgno] [Offset] [Raw]
    //     1       8     4       2

    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw){
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageCommon.getFSO(pg));
        return Bytes.concat(logTypeRaw,xidRaw,pgnoRaw,offsetRaw);
    }

    public static InsertLogInfo parseInsertLog(byte[] log){
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID,OF_INSERT_PGNO));
        insertLogInfo.pgno = Parser.parseInt(Arrays.copyOfRange(log,OF_INSERT_PGNO,OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log,OF_INSERT_OFFSET,OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log,OF_INSERT_RAW,log.length);
        return insertLogInfo;
    }

    public static void doInsertLog(PageCache pc, byte[] log, int flag){
        InsertLogInfo insertLogInfo = parseInsertLog(log);
        Page page = null;
        try {
            page = pc.getPage(insertLogInfo.pgno);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            if(flag == UNDO){
                DataItem.SetDataItemRawInvalid(insertLogInfo.raw);
            }
            PageCommon.recoverInsert(page,insertLogInfo.raw,insertLogInfo.offset);
        } finally {
            page.release();
        }
    }

}
