package mydb.backend.dm.page;

import mydb.backend.dm.PageCache.PageCache;
import mydb.backend.utils.RandomUtil;

import java.util.Arrays;

public class PageOne{
    private static final int OF_VC=100;
    private static final int LEN_VC=8;
    public static byte[] InitRaw(){
        byte[] raw=new byte[PageCache.pagesize];
         setVCOpen(raw);
        return raw;
    }
    public static void setVCOpen(Page pg){
        pg.setDirty(true);
        setVCOpen(pg.getData());

    }
    public static void setVCOpen(byte[] raw){
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }
    private static boolean checkVc(byte[] raw) {
        return Arrays.compare(raw, OF_VC, OF_VC+LEN_VC, raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC) == 0;
    }
}
