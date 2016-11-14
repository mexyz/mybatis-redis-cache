package com.xyz.mybatis.redis.cache.redisutil.task;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RList;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;


public class RedisListTask extends BaseTask implements Runnable {

  public RedisListTask(RedissonClient redissonClient, String listName, Collection<Object> collection, int type,
      int autoUnLockTime, Long timeToLive, TimeUnit timeUnit) {
    this.redissonClient = redissonClient;
    this.name = listName;
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
    RList<Object> list = redissonClient.getList(name);
    if (list != null) {
      if (type == 0) {
        for (Object obj : this.collection) {
          list.remove(obj);
        }
      } else if (type == -1) {
        list.delete();
      } else {
        for (Object obj : this.collection) {
          list.add(obj);
        }
      }
      if (timeToLive != null) {
        list.expire(timeToLive, timeUnit);
      }
    }
    lock.writeLock().unlock();

  }

}
