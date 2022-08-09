package mydb.backend.dm;

import mydb.backend.dm.PageCache.PageCache;
import mydb.backend.dm.dataItem.DataItem;
import mydb.backend.dm.logger.Logger;
import mydb.backend.dm.page.Page;
import mydb.backend.dm.page.PageOne;
import mydb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc =PageCache.create(path,mem);
        Logger lg=Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }
    public static DataManager open(String path,long mem,TransactionManager tm){
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVCOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
