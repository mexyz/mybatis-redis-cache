package com.xyz.mybatis.redis.cache.redisutil.task;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RBucket;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;


public class RedisBucketTask extends BaseTask implements Runnable {

  public RedisBucketTask(RedissonClient redissonClient, String bucketName, Collection<Object> collection, int type,
      int autoUnLockTime, Long timeToLive, TimeUnit timeUnit) {
    this.redissonClient = redissonClient;
    this.name = bucketName;
    this.autoUnLockTime = autoUnLockTime;
    this.collection = collection;
    this.type = type;
    this.timeToLive = timeToLive;
    this.timeUnit = timeUnit;
  }

  @Override
  public void run() {
    if(type==-1){
      for(Object obj:collection){
        String name=String.valueOf(obj);
        RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + name);
        lock.writeLock().lock(autoUnLockTime, TimeUnit.SECONDS);
        RBucket<Object> bucket = redissonClient.getBucket(name);
        if (bucket != null) {
          bucket.delete();
        }
        lock.writeLock().unlock();
      }
    }else{
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + name);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.SECONDS);
      RBucket<Object> bucket = redissonClient.getBucket(name);
      if (bucket != null) {
        for (Object obj : collection) {
          bucket.set(obj);
        }
        if (timeToLive != null) {
          bucket.expire(timeToLive, timeUnit);
        }
      }
      lock.writeLock().unlock();
    }
    
  }

}
