package com.xyz.mybatis.redis.cache.redisutil.task;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RMap;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;

public class RedisMapTask extends BaseTask implements Runnable {

  public RedisMapTask(RedissonClient redissonClient, String mapName, Map<String, Object> map, String key,
      int autoUnLockTime, Long timeToLive, TimeUnit timeUnit) {
    this.redissonClient = redissonClient;
    this.name = mapName;
    this.map = map;
    this.autoUnLockTime = autoUnLockTime;
    this.timeToLive = timeToLive;
    this.timeUnit = timeUnit;
    this.key = key;
  }

  @Override
  public void run() {
    RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + name);
    lock.writeLock().lock(autoUnLockTime, TimeUnit.SECONDS);
    RMap<Object, Object> map = redissonClient.getMap(name);
    if (map != null) {
      if (this.map == null) {
        if(key==null){
          map.delete();
        }else{
          map.fastRemove(key);
        }
        
      } else {
        for (Entry<String, Object> entry : this.map.entrySet()) {
          String key = String.valueOf(entry.getKey());
          Object obj = entry.getValue();
          if (obj == null) {
            map.fastRemove(key);
          } else {
            map.fastPut(key, obj);
          }
        }
      }

      if (timeToLive != null) {
        map.expire(timeToLive, timeUnit);
      }
    }

    lock.writeLock().unlock();
  }

}
