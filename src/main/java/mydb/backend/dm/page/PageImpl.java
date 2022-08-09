package mydb.backend.dm.page;

import mydb.backend.dm.PageCache.PageCache;
import mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page {
    private int pageNumber;
    private byte[] data;
    private boolean dirty;
    private Lock lock;
    private PageCache cache;
    public PageImpl(int pageNumber,byte[] data,PageCache cache){
        this.pageNumber=pageNumber;
        this.data=data;
        this.cache=cache;
        lock=new ReentrantLock();
    }
    @Override
    public void lock() {
        lock.lock();
    }
    @Override
    public void unlock() {
    lock.unlock();
    }
    @Override
    public void release() {
    cache.release(this);
    }
    @Override
    public void setDirty(boolean dirty) {
      this.dirty=dirty;
    }
    @Override
    public boolean isDirty() {
        return dirty;
    }
    @Override
    public int getPageNumber() {
        return pageNumber;
    }
    @Override
    public byte[] getData() {
        return data;
    }
}
