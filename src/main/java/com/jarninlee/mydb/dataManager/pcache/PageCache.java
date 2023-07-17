package com.jarninlee.mydb.dataManager.pcache;

import com.jarninlee.mydb.dataManager.page.Page;
import com.jarninlee.mydb.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache{
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pgno);
    void close();
    void release(Page page);
    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory) {
        System.out.println(path);
        File f = new File(path);
        if (f.exists()) {
            Panic.panic(new RuntimeException("file already exists1"));
        }
        try {
            f.createNewFile();
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("file can not read or write"));
        }

        FileChannel fc = null;
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fc = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int) memory / PAGE_SIZE, randomAccessFile,fc);
    }

    public static PageCacheImpl open(String path, long memory){
        File f = new File(path);
        if(!f.exists()){
            Panic.panic(new RuntimeException("file not exists!"));
        }
        if(!f.canWrite() || !f.canRead()){
            Panic.panic(new RuntimeException("file can read or write"));
        }
        FileChannel fc = null;
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fc = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int) memory / PAGE_SIZE, randomAccessFile,fc);
    }

}
