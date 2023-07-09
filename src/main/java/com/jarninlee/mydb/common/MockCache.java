package com.jarninlee.mydb.common;

public class MockCache extends AbstractCache<Long>{
    public MockCache() {
        super(5);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {

    }
}
