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

//实现计数的缓存策略
@Slf4j
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;         //实际缓存的数据
    private HashMap<Long, Boolean> getting; // 正在获取某资源的线程
    private HashMap<Long, Integer> references;         //资源的引用个数

    private int maxResource;                //缓存的最大缓存容量
    private int count = 0;                  //缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        lock = new ReentrantLock();
        cache = new HashMap<>();
        getting = new HashMap<>();
        references = new HashMap<>();
    }

    public T get(long key) {
        while (true) {
            lock.lock();
            //是否有其他线程正在获取资源
            if (getting.containsKey(key)) {
                lock.unlock();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            //计数策略  缓存区满 报错
            if (maxResource > 0 && maxResource == count) {
                lock.unlock();
//                Panic.panic(new RuntimeException("Cache is full !!!"));
                throw new RuntimeException("Cache is full !!!");
            }

            //如果缓冲区中不存在资源 且 未满，就进行线程的资源获取注册 转向其他方式获取
            count++;
            getting.put(key, true);
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
            count--;
            getting.remove(key);
            lock.unlock();
            log.info("该页不存在");
            Panic.panic(e);
        }

        lock.lock();

        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);

        lock.unlock();
        return obj;
    }

    protected void release(Long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if (ref == 0) {
                T obj = cache.get(key);
                if (obj == null) return;
                releaseForCache(obj);
                cache.remove(key);
                references.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    protected void close() {
        lock.lock();
        Set<Long> keySet = cache.keySet();

        for (Long aLong : keySet) {
            release(aLong);
            cache.remove(aLong);
            references.remove(aLong);
        }
        count = 0;
        lock.unlock();
    }

    /**
     * 缓存不包含所需资源时的方法
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 资源被驱逐
     *
     * @param obj
     */
    protected abstract void releaseForCache(T obj);
}
