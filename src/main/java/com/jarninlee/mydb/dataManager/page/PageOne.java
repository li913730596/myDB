package com.jarninlee.mydb.dataManager.page;

import com.jarninlee.mydb.dataManager.pcache.PageCache;
import com.jarninlee.mydb.utils.RandomUtil;

import java.util.Arrays;

public class PageOne {
    //VC : ValidCheck
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }
    //打开时在101-108字节处填入随机值
    public static void setVcOpen(Page pg){
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
    }

    public static void setVcClose(Page pg){
        pg.setDirty(true);  //保存一遍
        setVcClose(pg.getData());
    }
    //关闭时 在109-116处复制 100-108的内容，以便检查是否正常关闭
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw,OF_VC,raw,OF_VC + LEN_VC,LEN_VC);
    }

    public static boolean checkVc(Page pg){
        return check(pg.getData());
    }

    private static boolean check(byte[] raw){
        return Arrays.compare(raw,OF_VC,OF_VC + LEN_VC,   raw,OF_VC+LEN_VC,LEN_VC) == 0;
    }
}
