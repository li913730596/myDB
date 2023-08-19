package com.jarninlee.mydb.versionmanager;

import com.jarninlee.mydb.common.AbstractCache;
import com.jarninlee.mydb.dataManager.DataManager;
import com.jarninlee.mydb.transactionManager.TransactionManager;
import com.jarninlee.mydb.transactionManager.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    DataManager dm;
    TransactionManager tm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lockTable;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID,Transaction.newTransaction(TransactionManagerImpl.SUPER_XID,0,null));
        this.lock = new ReentrantLock();
        this.lockTable = new LockTable();
    }
 @Override
    public byte[] read(long xid, long uid) throws Exception {

        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if("null entry".equals(e.getMessage())) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {

        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {

        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if("null entry".equals(e.getMessage())) {
                return false;
            } else {
                throw e;
            }
        }
        try {

            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lockTable.add(xid, uid);
            } catch(Exception e) {
                t.err = new RuntimeException("concurrent update issue!");
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            l.lock();
            l.unlock();

            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = new RuntimeException("concurrent update issue!");
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lockTable.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = null;
        try {
            entry = Entry.loadEntry(this, uid);
        } catch(Exception e) {
            throw e;
        }
        if(entry == null) {
            throw new RuntimeException("null entry");
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

}
