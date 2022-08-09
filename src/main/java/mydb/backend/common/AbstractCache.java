package mydb.backend.common;

import mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractCache<T> {
    private HashMap<Long,T> cache;
    private HashMap<Long,Integer> references;
    private HashMap<Long,Boolean> getting;
    private int maxResource;
    private int count=0;
    private Lock lock;
    public AbstractCache(int maxResource){
        this.maxResource=maxResource;
        cache=new HashMap<>();
        references=new HashMap<>();
        getting=new HashMap<>();
        lock=new ReentrantLock();
    }
    protected T get(long key) throws Exception {
        while(true){
            lock.lock();
            if(getting.containsKey(key)){
                lock.unlock();
                try{
                    Thread.sleep(1);
                } catch (InterruptedException exception) {
                    exception.printStackTrace();
                    continue;
                }
                continue;
            }
            if(cache.containsKey(key)){
                T obj=cache.get(key);
                references.put(key, references.get(key)+1);
                lock.unlock();
                return obj;
            }
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key,true);
            lock.unlock();
            break;
        }
        T obj=null;
        try{
            obj=getForCache(key);
        }catch(Exception e){
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;

    }
    protected void release(long key){
        lock.lock();
        try{
            int ref=references.get(key)-1;
            if(ref==0){
                T obj=cache.get(key);
                releaseForCache(obj);
                cache.remove(obj);
                references.remove(obj);
                count--;
            }else{
                references.put(key,ref);
            }
            }finally{
            lock.unlock();
        }
    }
    protected void close(){
        lock.lock();
        try{
            Set<Long> keys=cache.keySet();
            for(long key:keys){
                release(key);
                references.remove(key);
                cache.remove(key);

            }
        }finally{
            lock.unlock();
        }
    }
    protected abstract T getForCache(long key) throws Exception;
    protected abstract void releaseForCache(T obj);
}
