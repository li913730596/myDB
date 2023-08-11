package com.jarninlee.mydb.dataManager.logger;

import com.google.common.primitives.Bytes;
import com.jarninlee.mydb.common.Parser;
import com.jarninlee.mydb.utils.Panic;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 * <p>
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * <p>
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
@Slf4j
public class LoggerImpl implements Logger {
    private static final int SEED = 13331;
    private static final int OF_SIZE = 0; // offset
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4; //TODO  这里含义是什么？ 每条日志中data的偏移量吗？

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;


    private Long postion; //当前文件指针
    private long fileSize;//初始化时记录，log操作不更新
    private int xCheckSum;

    LoggerImpl(RandomAccessFile file, FileChannel fc, int xCheckSum) {
        this.file = file;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    void init() {
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (length < 4) {
            Panic.panic(new RuntimeException("Bad log file"));
        }
        ByteBuffer raw = ByteBuffer.allocate(4);

        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(raw.array());
        this.fileSize = length;
        this.xCheckSum = xCheckSum;
        log.info("{} {}", fileSize, xCheckSum);

        checkAndRemoveTail();
    }

    //检查并移除badTail
    public void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while (true) {
            //返回的是检验完的完整日志
            byte[] log = internNext();
            if (log == null) break;
            System.out.println(xCheck);
            //计算每个日志的checksum
            xCheck = Parser.parseInt(calCheckSum(xCheck, log));
        }

        log.info("{}   {}", xCheck, xCheckSum);
        if (xCheck != xCheckSum) {
            Panic.panic(new RuntimeException("Bad log file"));
        }
        //在最后一个完整日志的部分进行截断
        truncate(postion);

        try {
            file.seek(postion); //将指针移动到postion位置
        } catch (IOException e) {
            Panic.panic(e);
        }

        rewind();
    }

    @Override
    public void log(byte[] data) {
        byte[] log = warpLog(data);// 创建日志

        ByteBuffer buffer = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buffer); // 存储至日志文件末尾
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateCheckSum(log); //更新文件的总checksum
    }

    private byte[] warpLog(byte[] data) {
        byte[] checkSum = calCheckSum(0, data); //checksum
        byte[] size = Parser.int2Byte(data.length); // size
        return Bytes.concat(size, checkSum, data); // log: [size, checksum, data]
    }

    private byte[] calCheckSum(int xCheck, byte[] data) {
        for (byte d : data) {
            xCheck = xCheck * SEED + d;
        }
        return Parser.int2Byte(xCheck);
    }

    private void updateCheckSum(byte[] log) {
        byte[] checkSum = calCheckSum(this.xCheckSum, log);
        this.xCheckSum = Parser.parseInt(checkSum);
        ByteBuffer buffer = ByteBuffer.wrap(checkSum);
        try {
            fc.position(0);
            fc.write(buffer);
            fc.force(true); //强制刷新
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] internNext() {
        if (postion + OF_DATA >= fileSize) {
            return null;
        }

        ByteBuffer tmp = ByteBuffer.allocate(4); //读size 4字节
        try {
            fc.position(postion);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }

        //该日志段的大小 size
        int size = Parser.parseInt(tmp.array());

        if (postion + OF_DATA + size > fileSize) {
            return null;
        }
        ByteBuffer data = ByteBuffer.allocate(size + OF_DATA);// 整体的日志 size + checksum + data 长度

        try {
            fc.position(postion);
            fc.read(data);
        } catch (IOException e) {
            Panic.panic(e);
        }

        byte[] log = data.array();//日志整体
        //Arrays.copyOfRange(log, OF_DATA, log.length) 日志data
        int checkSum0 = Parser.parseInt(calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length)));
        //日志记录的checksum
        int checkSum1 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        //比较checksum
        if (checkSum1 != checkSum0) {
            return null;
        }

        postion += log.length;

        return log;
    }

    @Override
    public void truncate(long x) {
        lock.lock();
        try {
            fc.truncate(x);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取下一条完整日志，返回的是日志的data部分
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        postion = 4L;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
