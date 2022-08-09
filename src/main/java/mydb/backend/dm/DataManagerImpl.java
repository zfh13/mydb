package mydb.backend.dm;

import mydb.backend.common.AbstractCache;
import mydb.backend.dm.PageCache.PageCache;
import mydb.backend.dm.dataItem.DataItem;
import mydb.backend.dm.dataItem.DataItemImpl;
import mydb.backend.dm.logger.Logger;
import mydb.backend.dm.page.Page;
import mydb.backend.dm.page.PageOne;
import mydb.backend.dm.page.PageX;
import mydb.backend.dm.pageIndex.PageIndex;
import mydb.backend.dm.pageIndex.PageInfo;
import mydb.backend.tm.TransactionManager;
import mydb.backend.utils.Panic;
import mydb.backend.utils.Types;
import mydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;
    public DataManagerImpl(PageCache pc,Logger logger,TransactionManager tm){
        super(0);
        this.pc=pc;
        this.logger=logger;
        this.tm=tm;
        this.pIndex=new PageIndex();
    }
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }
    public long insert(long xid,byte[] data)throws Exception{
        byte[] raw=DataItem.wrapdataitemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }
        PageInfo pi=null;
        for(int i=0;i<5;i++){
            pi=pIndex.select(raw.length);
            if(pi!=null){
                break;
            }else{
                int newPgno=pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }
        Page pg=null;
        int freeSpace=0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }
    public void logDataItem(long xid,DataItem di){
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }
    void initPageOne(){
        int pgno= pc.newPage(PageOne.InitRaw());
        assert pgno==1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

}
