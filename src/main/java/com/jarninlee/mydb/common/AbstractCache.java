package com.jarninlee.mydb.common;

import com.jarninlee.mydb.utils.Panic;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//实现LRU的缓存策略   一个哈希表和一个双向链表
@Slf4j
public abstract class AbstractCache<T>{
    private HashMap<Long, T> cache;         //实际缓存的数据
    private LinkedList<Long> cacheKeyList;  //表述使用的时间链表
    private ConcurrentHashMap<Long, Boolean> getting; // 正在获取某资源的线程

    private int maxResource;                //缓存的最大缓存容量
    private Lock lock;

    public AbstractCache(int maxResource){
        this.maxResource = maxResource;
        lock = new ReentrantLock();
        cache = new HashMap<>();
        cacheKeyList = new LinkedList<>();
        getting = new ConcurrentHashMap<>();
    }

    public T get(long key){
        while (true){
            lock.lock();
            if(getting.containsKey(key)){
                lock.unlock();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            //TODO  这里难道不需要 将key添加进入getting吗？  万一我在使用时，被其他线程修改了怎么办？

            if (cache.containsKey(key)){
                T obj = cache.get(key);
                cacheKeyList.remove(key);
                cacheKeyList.addFirst(key);
                lock.unlock();
                return obj;
            }

            getting.put(key,true);
            lock.unlock();
            break;
        }

        /*
            缓存中不存在数据
         */
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            getting.remove(key);
            lock.unlock();
            Panic.panic(e);
        }

        lock.lock();
        getting.remove(key);
        if (cache.size() == maxResource) {
            //资源已满，需丢弃一个   链表和哈希表都需要丢弃
            release(cacheKeyList.getLast());
        }
        //存入缓存  也是两步
        cache.put(key,obj);
        cacheKeyList.addFirst(key);


        lock.unlock();
        return obj;
    }

    protected void release(Long key){
        lock.lock();
        try {
            T obj = cache.get(key);
            if(obj == null) return;
            releaseForCache(obj);
            cache.remove(obj);
            cacheKeyList.remove(obj);
        } finally {
            lock.unlock();
        }
    }

    protected void close(){
        lock.lock();
        Set<Long> keySet = cache.keySet();

        for (Long aLong : keySet) {
            release(aLong);
        }
        lock.unlock();
    }

    /**
     * 缓存不包含所需资源时的方法
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 资源被驱逐
     * @param obj
     */
    protected abstract void releaseForCache(T obj);
}
