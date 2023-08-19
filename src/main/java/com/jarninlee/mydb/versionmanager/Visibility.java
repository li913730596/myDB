package com.jarninlee.mydb.versionmanager;

import com.jarninlee.mydb.transactionManager.TransactionManager;

public class Visibility {

    //TODO：这里的版本跳跃有必要处理吗？
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e){
        long xmax = e.getXmax();
        if (t.level == 0){
            return false;
        }else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e){
        if(t.level == 0){
            return readCommitted(tm,t,e);
        }else {
            return repeatableRead(tm,t,e);
        }
    }

    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin)){
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xid)){
            if(xmax == 0) return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)){
                    return true;
                }
            }
        }
        return false;
    }
}
