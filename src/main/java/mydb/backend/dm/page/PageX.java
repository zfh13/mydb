package mydb.backend.dm.page;

import mydb.backend.dm.PageCache.PageCache;
import mydb.backend.utils.Parser;

import java.util.Arrays;

public class PageX {
    private static final short OF_FREE=0;
    private static final short OF_DATA=2;
    public static final int MAX_FREE_SPACE = PageCache.pagesize - OF_DATA;
    public static byte[] initRaw(){
        byte[] raw=new byte[PageCache.pagesize];
        return raw;
    }
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }
    public static short getFSO(Page pg){
        return getFSO(pg.getData());
        }
    private static short getFSO(byte[] raw){
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }
    public static short insert(Page pg,byte[] raw){
        pg.setDirty(true);
        short offset=getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(),(short)(offset+ raw.length));
        return offset;
    }
    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.pagesize - (int)getFSO(pg.getData());
    }
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }

}
