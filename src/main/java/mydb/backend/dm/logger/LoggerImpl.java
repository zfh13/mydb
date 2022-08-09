package mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import mydb.backend.dm.logger.Logger;
import mydb.backend.utils.Panic;
import mydb.backend.utils.Parser;
import mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;
    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }
    void init(){
        long size=0;
        try{
            size=file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size<4){
            Panic.panic(Error.BadLogFileException);
        }
        ByteBuffer raw=ByteBuffer.allocate(4);
        try{
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum= Parser.parseInt(raw.array());
        this.fileSize=size;
        this.xChecksum=xChecksum;
        checkAndRemoveTail();
    }
    private void checkAndRemoveTail(){
        rewind();
        int xCheck=0;
        while(true){
            byte[] log=internNext();
            if(log==null){
                break;
            }
            xCheck=calCheckSum(xCheck,log);
        }
        if(xCheck!=xChecksum){
            Panic.panic(Error.BadLogFileException);
        }
        try{
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }
    public int calCheckSum(int xCheck,byte[] log){
        for(byte b:log){
            xCheck=xCheck*SEED+b;
        }
        return xCheck;
    }
    @Override
    public void log(byte[] data) {
     byte[] log=wraplog(data);
     ByteBuffer buf=ByteBuffer.wrap(log);
     lock.lock();
     try{
         fc.position(fc.size());
         fc.write(buf);
     } catch (IOException e){
         Panic.panic(e);
     }finally{
         lock.unlock();
     }
     updateXChecksum(log);
    }
  private byte[] wraplog(byte[] data){
        byte[] checksum=Parser.int2Byte(calCheckSum(0,data));
        byte[] size=Parser.int2Byte(data.length);
        return Bytes.concat(size,checksum,data);
  }
    @Override
    public void truncate(long x) throws Exception {
          lock.lock();
          try{
              fc.truncate(x);
          } catch (IOException e) {
              Panic.panic(e);
          }
          lock.unlock();

    }
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calCheckSum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    @Override
    public byte[] next() {
        lock.lock();
        try{
            byte[] log=internNext();
            if(log==null){
                return null;
            }
            return Arrays.copyOfRange(log,OF_DATA,log.length);
            }finally{
            lock.unlock();

        }
    }
  public byte[] internNext(){
      if(position + OF_DATA >= fileSize) {
          return null;
      }
      ByteBuffer tmp = ByteBuffer.allocate(4);
      try {
          fc.position(position);
          fc.read(tmp);
      } catch(IOException e) {
          Panic.panic(e);
      }
      int size = Parser.parseInt(tmp.array());
      if(position + size + OF_DATA > fileSize) {
          return null;
      }

      ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
      try {
          fc.position(position);
          fc.read(buf);
      } catch(IOException e) {
          Panic.panic(e);
      }
      byte[] log=buf.array();
      int checkSum1=calCheckSum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
      int checkSum2=Parser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA));
      if(checkSum1!=checkSum2){
          return null;
      }
      position+=log.length;
      return log;
  }
    @Override
    public void rewind() {
     position=4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
}
