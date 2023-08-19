package com.jarninlee.mydb.transactionManager;

import com.jarninlee.mydb.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.jarninlee.mydb.transactionManager.TransactionManagerImpl.XID_HEADER_LENGTH;

public interface TransactionManager {

    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();



    public static TransactionManagerImpl create(String path){
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()){
                Panic.panic(new RuntimeException("file already exists!"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(!f.canRead() || !f.canWrite()){
            Panic.panic(new RuntimeException("file cannot read or write"));
        }

        FileChannel fc = null;

        RandomAccessFile randomAccessFile;
        try {
            randomAccessFile = new RandomAccessFile(f, "rw");
            fc = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        //写空文件头
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);

        try {
            fc.position(0);
            fc.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException("非法的XID文件");
        }

        return new TransactionManagerImpl(randomAccessFile,fc);
    }

    public static TransactionManagerImpl open(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);

        if(!file.exists()){
            Panic.panic(new RuntimeException("file not exists"));
        }

        if(!file.canRead() || !file.canWrite()){
            Panic.panic(new RuntimeException("file can not read or write"));
        }

        FileChannel fc = null;
        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(file,"rw");
            fc = randomAccessFile.getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        return new TransactionManagerImpl(randomAccessFile,fc);

    }
}
