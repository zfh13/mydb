package mydb.backend.im;

import mydb.backend.common.SubArray;
import mydb.backend.dm.DataManager;
import mydb.backend.dm.dataItem.DataItem;
import mydb.backend.tm.TransactionManagerImpl;
import mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;
    public static long create(DataManager dm)throws Exception{
        byte[] rawRoot=Node.newNilRootRaw();
        long rootUid=dm.insert(TransactionManagerImpl.SUPER_XID,rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }
    public static BPlusTree load(long bootUid,DataManager dm)throws Exception{
        DataItem bootDataItem =dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }
    private void updateRootUid(long left,long right,long rightKey)throws Exception{
        bootLock.lock();
        try{
            byte[] rootRaw=Node.newRootRaw(left,right,rightKey);
            long newrootUid= dm.insert(TransactionManagerImpl.SUPER_XID,rootRaw);
            bootDataItem.before();
            SubArray diRaw= bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newrootUid),0,diRaw.raw,diRaw.start,8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        }finally{
            bootLock.unlock();
        }
    }
    private long searchLeaf(long nodeUid,long key)throws Exception{
        Node node=Node.loadNode(this,nodeUid);
        boolean isLeaf=node.isLeaf();
        node.release();
        if(isLeaf){
            return nodeUid;
        }else{
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }
    private long searchNext(long nodeUid,long key)throws Exception{
        while(true){
            Node node=Node.loadNode(this,nodeUid);
            Node.SearchNextRes res=node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }
    public List<Long> searchRange(long leftkey,long rightkey)throws Exception{
        long rootUid=rootUid();
        long leafUid=searchLeaf(rootUid,leftkey);
        List<Long> uids=new ArrayList<>();
        while(true){
            Node leaf=Node.loadNode(this,leafUid);
            Node.LeafSearchRangeRes res=leaf.leafSearchRange(leftkey, rightkey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }
    public void insert(long key,long uid)throws Exception{
        long rootUid=rootUid();
        InsertRes res = insert(rootUid, uid, key);

    }
    class InsertRes {
        long newNode, newKey;
    }
    private InsertRes insert(long nodeUid,long uid,long key)throws Exception{
        Node node=Node.loadNode(this,nodeUid);
        boolean isLeaf=node.isLeaf();
        node.release();
        InsertRes res=null;
        if(isLeaf){
            res = insertAndSplit(nodeUid, uid, key);
        }else{
            long next=searchNext(nodeUid,key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode!=0){
                res=insertAndSplit(nodeUid,ir.newNode,ir.newKey);

            }else{
                res=new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid,long uid ,long key)throws Exception{
           while(true){
               Node node=Node.loadNode(this,nodeUid);
               Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
               node.release();
               if(iasr.siblingUid != 0) {
                   nodeUid = iasr.siblingUid;
               } else {
                   InsertRes res = new InsertRes();
                   res.newNode = iasr.newSon;
                   res.newKey = iasr.newKey;
                   return res;
               }

           }
    }

    public void close() {
        bootDataItem.release();
    }
}
