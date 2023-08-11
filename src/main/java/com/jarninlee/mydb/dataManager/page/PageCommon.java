package com.jarninlee.mydb.dataManager.page;

import ch.qos.logback.core.net.ssl.SSL;
import com.jarninlee.mydb.common.Parser;
import com.jarninlee.mydb.dataManager.pcache.PageCache;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageCommon {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    private static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }
    //前两字节 代表FreeSpaceOffset  偏移量
    private static void setFSO(byte[] raw, short ofData){
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    public static short getFSO(Page pg){
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw){
        return Parser.parseShort(Arrays.copyOfRange(raw,OF_FREE,OF_DATA));
    }

    //将raw插入page,返回插入位置
    public short insert(Page pg, int[] raw){
        pg.setDirty(true);
        short offset = getFSO(pg);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length)); //java中  两个short加起来  会转化为int
        return offset;
    }

    //获取剩余可用空间大小
    public static int getFreeSpace(Page pg){
        return PageCache.PAGE_SIZE - (int)getFSO(pg);
    }

    //将raw插入pg中的offset位置，并将pg的offset设置为较大的offset  （直接修改前两个字节的offset字段）
    public void recoverInsert(Page pg, byte[] raw, short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        short of = getFSO(pg);
        if(of < offset + raw.length) {
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    // 将raw插入pg中的offset位置，不更新update   更新数据长度没变  所以不用setFSO更新空闲偏移量
    public void recoverUpdate(Page pg, byte[] raw, short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
    }

}
