package com.xyz.mybatis.redis.cache.redisutil.task;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RReadWriteLock;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;


public class RedisSetTask extends BaseTask implements Runnable {

  public RedisSetTask(RedissonClient redissonClient, String setName,Collection<Object> collection, int type, int autoUnLockTime,
      Long timeToLive, TimeUnit timeUnit) {
    this.redissonClient = redissonClient;
    this.name = setName;
    this.autoUnLockTime = autoUnLockTime;
    this.collection = collection;
    this.type = type;
    this.timeToLive = timeToLive;
    this.timeUnit = timeUnit;
  }

  @Override
  public void run() {
    RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + name);
    lock.writeLock().lock(autoUnLockTime, TimeUnit.SECONDS);
    RSet<Object> set = redissonClient.getSet(name);
    if (set != null) {
      if (type == 0) {
        for (Object obj : this.collection) {
          set.remove(obj);
        }
      } else if (type == -1) {
        set.delete();
      } else {// 添加
        for (Object obj : this.collection) {
          set.add(obj);
        }
      }
      if (timeToLive != null) {
        set.expire(timeToLive, timeUnit);
      }
    }
    lock.writeLock().unlock();

  }

}
