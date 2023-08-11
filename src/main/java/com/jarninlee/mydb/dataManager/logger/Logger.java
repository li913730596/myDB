package com.jarninlee.mydb.dataManager.logger;

import com.jarninlee.mydb.common.Parser;
import com.jarninlee.mydb.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    void log(byte[] data);
    void truncate(long x);
    byte[] next();
    void rewind();
    void close();


    public static Logger create(String path){
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(new RuntimeException("File already exists!"));
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("File cannot read or write"));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        ByteBuffer buffer = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buffer);
            fc.force(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new LoggerImpl(raf,fc,0);
    }


    public static Logger open(String path){
        File f = new File(path + LoggerImpl.LOG_SUFFIX);

        if(!f.exists()){
            Panic.panic(new RuntimeException("file not exists"));
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(new RuntimeException("File cannot read or write"));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        LoggerImpl logger = new LoggerImpl(raf, fc);
        logger.init();

        return logger;
    }

}
