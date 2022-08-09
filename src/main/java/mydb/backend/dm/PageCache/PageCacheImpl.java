package mydb.backend.dm.PageCache;

import mydb.backend.common.AbstractCache;
import mydb.backend.dm.page.Page;
import mydb.backend.dm.page.PageImpl;
import mydb.backend.utils.Panic;
import mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
     private static final int  MEM_MIN_LIM=10;
     public static final String DB_SUFFIX=".db";
     private RandomAccessFile file;
     private FileChannel fc;
     private Lock filelock;
     private AtomicInteger pageNumbers;
     PageCacheImpl(RandomAccessFile file,FileChannel fileChannel,int maxResource){
         super(maxResource);
         if(maxResource < MEM_MIN_LIM) {
             Panic.panic(Error.MemTooSmallException);
         }
        long length=0;
         try{
             length= file.length();
         } catch (IOException e) {
             Panic.panic(e);
         }
         this.file=file;
         this.fc=fileChannel;
         this.filelock=new ReentrantLock();
         this.pageNumbers=new AtomicInteger((int)length/pagesize);
     }

    @Override
    protected Page getForCache(long key) throws Exception {
       int pgno=(int) key;
       long offset=PageCacheImpl.pageOffset(pgno);
       ByteBuffer buf=ByteBuffer.allocate(pagesize);
       filelock.lock();
       try{
           fc.position(offset);
           fc.read(buf);
       }catch (IOException e){
           Panic.panic(e);
       }
         filelock.unlock();
       return new PageImpl(pgno,buf.array(),this);
    }

    @Override
    protected void releaseForCache(Page pg) {
    if(pg.isDirty()){
        flush(pg);
        pg.setDirty(false);
    }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno=pageNumbers.incrementAndGet();
        Page page=new PageImpl(pgno,initData,null);
        flush(page);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    @Override
    public void release(Page page) {
      release((long)page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
     long size=pageOffset(maxPgno+1);
     try{
         file.setLength(size);
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
    public void flushPage(Page page) {
       flush(page);
    }
    public void flush(Page pg){
         int pgno=pg.getPageNumber();
         long offset=pageOffset(pgno);
         filelock.lock();
         try{
             ByteBuffer buff=ByteBuffer.wrap(pg.getData());
             fc.position(offset);
             fc.write(buff);
             fc.force(false);
         } catch (IOException e) {
             Panic.panic(e);
         }finally{
             filelock.unlock();
         }
    }
    public void close() {
        super.close();
        try{
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }

    }
    private static long pageOffset(int pgno) {
        return (pgno-1) * pagesize;
    }


}
