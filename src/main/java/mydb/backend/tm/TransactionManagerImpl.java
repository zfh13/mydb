package mydb.backend.tm;


import mydb.backend.utils.Panic;
import mydb.backend.utils.Parser;
import mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {
    static final int LEN_XID_HEADER_LENGTH=8;
    private static final int XID_FIELD_SIZE=1;
    private static final byte FIELD_TRAN_ACTIVE=0;
    private static final byte FIELD_TRAN_COMMITED=1;
    private static final byte FILED_TRAN_ABORTED=2;
    public static final long SUPER_XID=0;
    static final String XID_SUFFIX=".xid";
    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterlock;
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc){
        this.file=raf;
        this.fc=fc;
        counterlock=new ReentrantLock();
        checkXIDCounter();
    }

   private void checkXIDCounter(){
       long filelen=0;
       try{
           filelen=file.length();

       } catch (IOException e) {
           Panic.panic(Error.BadXIDFileException);
       }
       if(filelen<LEN_XID_HEADER_LENGTH){
           Panic.panic(Error.BadXIDFileException);
       }
       ByteBuffer buff= ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
       try{
           fc.position(0);
           fc.read(buff);
       } catch (IOException e) {
           Panic.panic(e);
       }
       this.xidCounter= Parser.parseLong(buff.array());
       long end = getXidPosition(this.xidCounter + 1);
       if(end != filelen) {
           Panic.panic(Error.BadXIDFileException);
       }
   }
   private long getXidPosition(long xid){
       return LEN_XID_HEADER_LENGTH+(xid-1)*XID_FIELD_SIZE;
   }
   private void updateXID(long xid,byte status){
       long offset=getXidPosition(xid);
       byte[] tmp=new byte[XID_FIELD_SIZE];
       tmp[0]=status;
       ByteBuffer buf=ByteBuffer.wrap(tmp);
       try{
           fc.position(offset);
           fc.write(buf);
       } catch (IOException e) {
           Panic.panic(e);
       }
       try{
           fc.force(false);
       } catch (IOException e) {
           Panic.panic(e);
       }
   }
   private void incrXIDCounter(){
       xidCounter++;
       ByteBuffer buf=ByteBuffer.wrap(Parser.long2Byte(xidCounter));
       try{
           fc.position(0);
           fc.write(buf);
       } catch (IOException e) {
            Panic.panic(e);
       }
       try{
           fc.force(false);
       } catch (IOException e) {
           Panic.panic(e);
       }
   }
   public long begin()
   {
       counterlock.lock();
      try{
          long xid=xidCounter+1;
          updateXID(xid,FIELD_TRAN_ACTIVE);
          incrXIDCounter();
          return xid;
      }finally{
          counterlock.unlock();
      }
   }
   public void commit(long xid){
       updateXID(xid,FIELD_TRAN_COMMITED);

   }
   public void abort(long xid){
       updateXID(xid,FILED_TRAN_ABORTED);
   }
   private boolean checkXID(long xid,byte status){
       long offset=getXidPosition(xid);
       ByteBuffer buf=ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
       try {
           fc.position(offset);
           fc.read(buf);
       } catch (IOException e) {
           Panic.panic(e);
       }
       return buf.array()[0]==status;
   }
   public boolean isActive(long xid){
       if(xid==SUPER_XID) {
           return false;
       }
       return checkXID(xid,FIELD_TRAN_ACTIVE);

   }
   public boolean isCommitted(long xid){
       if(xid==SUPER_XID) return true;
       return checkXID(xid,FIELD_TRAN_COMMITED);
   }
    public boolean isAborted(long xid){
        if(xid==SUPER_XID) return false;
        return checkXID(xid,FILED_TRAN_ABORTED);
    }
    public void close(){
       try{
           fc.close();
           file.close();

       } catch (IOException e) {
           Panic.panic(e);
       }
    }
}
