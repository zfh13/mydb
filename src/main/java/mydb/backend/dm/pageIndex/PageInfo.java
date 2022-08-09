package mydb.backend.dm.pageIndex;

public class PageInfo {
    public int pgno;
    public int freespace;
    public PageInfo(int pgno,int freespace){
        this.pgno=pgno;
        this.freespace=freespace;
    }
}
