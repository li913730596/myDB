package com.jarninlee.mydb.transactionManager;

import com.jarninlee.mydb.common.Parser;
import com.jarninlee.mydb.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.RandomAccess;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManager {

    //XID文件头长度
    private static final int XID_HEADER_LENGTH = 8;
    //每个事务占用长度
    private static final int XID_FIELD_LENGTH = 1;
    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    //超级事务，永远为commited状态
    private static final long SUPER_XID = 0;

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;



    private TransactionManager(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    public static TransactionManager create(String path){
        File f = new File(path);
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

        return new TransactionManager(randomAccessFile,fc);
    }

    /**
     * 检查XID是否合法
     * 读取XID_FILE_HEADER中的xidCounter,根据文件的理论长度，对比实际长度
     */
    private void checkXIDCounter(){
        long fileLength = 0;

        try {
            fileLength = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        if(fileLength < XID_HEADER_LENGTH){
            Panic.panic(new RuntimeException("非法的XID文件"));
        }

        ByteBuffer buffer = ByteBuffer.allocate(XID_HEADER_LENGTH);

        try {
            fc.position(0);
            fc.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.xidCounter = Parser.parseLong(buffer.array());
        // 加1是因为  0号代表超级事务 不用记录下来。
        // 所以后面计算的时候总个数需要减去一个，这里加1进行抵消。
        long end = getXidPosition(this.xidCounter + 1);

        if(end != fileLength){
            Panic.panic(new RuntimeException("非法的XID文件"));
        }

    }

    private long getXidPosition(long xid){
        return XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_LENGTH;
    }

    public long begin(){
        long xid = this.xidCounter + 1;
        updateXID(xid, FIELD_TRAN_ACTIVE);
        incrXIDCounter();
        return xid;
//        try {
//            counterLock.lock();
//            long xid = this.xidCounter + 1;
//            updateXID(xid, FIELD_TRAN_ACTIVE);
//            incrXIDCounter();
//            return xid;
//        } catch (Exception e) {
//            Panic.panic(e);
//        }finally {
//            counterLock.unlock();
//        }
//        return 0;
    }

    public void commit(long xid){
        long position = getXidPosition(xid);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{FIELD_TRAN_COMMITTED});


        try {
            fc.position(position);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public void abort(long xid){
        System.out.println("abort");
        long position = getXidPosition(xid);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{FIELD_TRAN_ABORTED});

        try {
            fc.position(position);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public boolean isCommited(long xid){
       if (xid == SUPER_XID) return true;
       return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isActive(long xid){
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isAbort(long xid){
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close(){
        try {
            fc.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public boolean checkXID(long xid, byte status){
        long position = getXidPosition(xid);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_LENGTH]);
        try {
            fc.position(position);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (buffer.array()[0] == status) {
            return true;
        }
        return false;
    }

    private void updateXID(long xid, byte status){
        long position = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_LENGTH];
        tmp[0] = status;
        ByteBuffer buffer = ByteBuffer.wrap(tmp);

        try {
            //利用ByteBuf中转 使用FileChannel 向file中写入
            fc.position(position);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            //用于将通道关联的文件的内容和元数据强制刷新到磁盘上的存储设备;   这里将其关闭
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void incrXIDCounter(){
        xidCounter ++;
        byte[] bytes = Parser.long2byte(xidCounter);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            fc.position(0);
            fc.write(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
