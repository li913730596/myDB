package com.jarninlee.mydb.dataManager.pcache;

import com.jarninlee.mydb.common.AbstractCache;
import com.jarninlee.mydb.dataManager.page.Page;
import com.jarninlee.mydb.dataManager.page.PageImpl;
import com.jarninlee.mydb.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static int MEM_MIN_LEN = 10;

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;
    private AtomicInteger pageNumbers;

    public PageCacheImpl(int maxResource, RandomAccessFile file, FileChannel fc) {
        super(maxResource);
        if (maxResource < MEM_MIN_LEN){
            Panic.panic(new RuntimeException("memory too small!"));
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        pageNumbers = new AtomicInteger(1);
    }



    @Override
    protected Page getForCache(long key) throws Exception {
        int pgNum = (int) key;
        long offSet = getOffSet(pgNum);
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);

        fileLock.lock();

        fc.position(offSet);
        fc.read(buffer);

        fileLock.unlock();

        return new PageImpl(pgNum, buffer.array(), this);
    }

    public long getOffSet(int pgNum) {
        return pgNum * (PAGE_SIZE);
    }

    @Override
    protected void releaseForCache(Page obj) {
        if (obj.isDirty()) {
            flushPage(obj);
            obj.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgNu = pageNumbers.getAndIncrement();
        Page page = new PageImpl(pgNu, initData, this);
        flushPage(page);

        return pgNu;
    }

    @Override
    public Page getPage(int pgno) {
        Page page = get(pgno);
        return page;
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }


    //TODO :  作用?  设置长度来截断文件
    @Override
    public void truncateByPgno(int maxPgno) {
        long offSet = getOffSet(maxPgno + 1);
        try {
            file.setLength(offSet);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        int pageNumber = pg.getPageNumber();
        long offSet = getOffSet(pageNumber);

        fileLock.lock();
        try {
            ByteBuffer buffer = ByteBuffer.wrap(pg.getData());
            fc.position(offSet);
            fc.write(buffer);
            fc.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }

    }


}
