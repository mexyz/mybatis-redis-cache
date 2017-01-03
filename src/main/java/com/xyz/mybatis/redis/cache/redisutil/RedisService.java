package com.xyz.mybatis.redis.cache.redisutil;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RBucket;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class RedisService {
  private static Logger logger = Logger.getLogger(RedisService.class);
  private static Config config = null;
  @Value("${redis.clusterIp:}")
  private String clusterIp;
  @Value("${redis.masterConnectionPoolSize:1000}")
  private int masterConnectionPoolSize;
  @Value("${redis.slaveConnectionPoolSize:1000}")
  private int slaveConnectionPoolSize;
  @Value("${redis.masterConnectionMinimumIdleSize:100}")
  private int masterConnectionMinimumIdleSize;
  @Value("${redis.slaveConnectionMinimumIdleSize:100}")
  private int slaveConnectionMinimumIdleSize;
  @Value("${redis.autoUnLock:1000}")
  private int autoUnLockTime;
  @Value("${redis.connectTimeout:1000}")
  private int connectTimeout;
  @Value("${redis.slaveRetryTimeout:3000}")
  private int slaveRetryTimeout;
  private RedissonClient redissonClient;

  @PostConstruct
  private void init() {
    if("".equals(clusterIp)||clusterIp == null){
      logger.error("redis.cluster.ip没有配置");
      return;
    }
    String[] ipPorts = null;
    try {
      ipPorts = clusterIp.split(",");
      config = new Config();
      ClusterServersConfig csc = config.useClusterServers();
      csc.setMasterConnectionPoolSize(masterConnectionPoolSize);
      csc.setSlaveConnectionPoolSize(slaveConnectionPoolSize);
      csc.setMasterConnectionMinimumIdleSize(masterConnectionMinimumIdleSize);
      csc.setSlaveConnectionMinimumIdleSize(slaveConnectionMinimumIdleSize);
      csc.setConnectTimeout(connectTimeout);
      csc.setReconnectionTimeout(slaveRetryTimeout);
      for (String ipPort : ipPorts) {
        csc.addNodeAddress(ipPort);
      }
    } catch (Exception e) {
      logger.error("redis.cluster.ip配置不正确");
    }

    try {
      redissonClient = Redisson.create(config);
    } catch (Exception e) {
      logger.error("redis链接失败");
    }
  }

  public <T> T getObject(String key) {
    if (key == null) {
      throw new RuntimeException("key不可为null");
    } else {
      RBucket<T> bucket = redissonClient.getBucket(key);
      if (bucket == null) {
        return null;
      } else {
        return bucket.get();
      }
    }

  }

  /**
   * 获得list集合
   * 
   * @param listName
   * @return
   */
  public <T> List<T> getList(String listName) {
    if (listName == null) {
      throw new RuntimeException("listName不可为null");
    } else {
      List<T> list = redissonClient.getList(listName);
      return list;
    }

  }

  /**
   * 根据key获得map的value
   * 
   * @param mapName
   * @return
   */
  public <K, V> Map<K, V> getMap(String mapName) {
    if (mapName == null) {
      throw new RuntimeException("mapName不可为null");
    } else {
      Map<K, V> map = redissonClient.getMap(mapName);
      return map;
    }
  }

  /**
   * 获得set集合
   * 
   * @param setName
   * @return
   */
  public <V> Set<V> getSet(String setName) {
    if (setName == null) {
      throw new RuntimeException("setName不可为null");
    } else {
      Set<V> set = redissonClient.getSet(setName);
      return set;
    }

  }

  /**
   * 获得Bucket
   * 
   * @param bucketName
   * @return
   */
  @Deprecated
  public <T> RBucket<T> getBucket(String bucketName) {
    if (bucketName == null) {
      throw new RuntimeException("bucketName不可为null");
    } else {
      RBucket<T> bucket = redissonClient.getBucket(bucketName);
      return bucket;
    }

  }

  /**
   * 添加/修改set
   * 
   * @param setName
   *          set集合名称
   * @param obj
   *          插入的对象
   */
  public void setSet(String setName, Object obj) {
    if (setName != null && obj != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + setName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RSet<Object> set = redissonClient.getSet(setName);
      if (set != null) {
        set.addAsync(obj);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("setName,obj不可为null");
    }
  }

  /**
   * 
   * @see RedisService.setSet(String setName, Object obj)
   * @param timeToLive
   *          保存时间
   * @param timeUnit
   *          保存时间的单位
   */
  public void setSet(String setName, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (setName != null && obj != null && timeToLive != null && timeUnit != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + setName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RSet<Object> set = redissonClient.getSet(setName);
      if (set != null) {
        set.addAsync(obj);
        set.expire(timeToLive, timeUnit);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("setName,obj,timeToLive,timeUnit不可为null");
    }

  }

  /**
   * 删除set
   * 
   * @param setName
   *          set集合名称
   * @param obj
   *          要删除的对象
   */
  public void delSet(String setName, Object obj) {
    if (setName != null && obj != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + setName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RSet<Object> set = redissonClient.getSet(setName);
      if (set != null) {
        set.removeAsync(obj);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("setName,obj不可为null");
    }

  }

  public void delSet(String setName) {
    if (setName != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + setName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RSet<Object> set = redissonClient.getSet(setName);
      if (set != null) {
        set.deleteAsync();
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("setName不可为null");
    }

  }

  /**
   * 添加/修改list
   * 
   * @param listName
   *          list集合名称
   * @param obj
   *          对象
   */
  public void setList(String listName, Object obj) {
    if (listName != null && obj != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + listName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RList<Object> list = redissonClient.getList(listName);
      if (list != null && obj != null) {
        list.addAsync(obj);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("listName,obj不可为null");
    }

  }

  /**
   * 
   * @see RedisService.setRList(String setName, Object obj)
   * @param timeToLive
   *          保存时间
   * @param timeUnit
   *          保存时间的单位
   */
  public void setList(String listName, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (listName != null && obj != null && timeToLive != null && timeUnit != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + listName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RList<Object> list = redissonClient.getList(listName);
      if (list != null && obj != null) {
        list.addAsync(obj);
        if (timeToLive != null) {
          list.expire(timeToLive, timeUnit);
        }
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("listName,obj,timeToLive,timeUnit不可为null");
    }

  }

  /**
   * 删除list
   * 
   * @param listName
   *          delRList
   * @param obj
   *          对象
   */
  public void delList(String listName, Object obj) {
    if (listName != null && obj != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + listName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RList<Object> list = redissonClient.getList(listName);
      if (list != null) {
        list.removeAsync(obj);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("listName,obj不可为null");
    }

  }

  public void delList(String listName, Integer index) {
    if (listName != null && index != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + listName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RList<Object> list = redissonClient.getList(listName);
      if (list != null) {
        list.removeAsync(index);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("listName,obj不可为null");
    }

  }

  public void delList(String listName) {
    if (listName != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + listName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RList<Object> list = redissonClient.getList(listName);
      if (list != null) {
        list.deleteAsync();
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("listName不可为null");
    }

  }

  public void setObject(String key, Object obj) {
    if (key != null && obj != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + key);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RBucket<Object> bucket = redissonClient.getBucket(key);
      if (bucket != null) {
        bucket.setAsync(obj);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("key,obj不可为null");
    }

  }

  public void setObject(String key, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (key != null && obj != null && timeToLive != null && timeUnit != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + key);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RBucket<Object> bucket = redissonClient.getBucket(key);
      if (bucket != null) {
        bucket.setAsync(obj);
        if (timeToLive != null) {
          bucket.expire(timeToLive, timeUnit);
        }
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("key,obj,timeToLive,timeUnit不可为null");
    }

  }
  
  public void setObject(String key, Object obj, Date date) {
    if (key != null && obj != null && date != null ) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + key);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RBucket<Object> bucket = redissonClient.getBucket(key);
      if (bucket != null) {
        bucket.setAsync(obj);
        bucket.expireAt(date);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("key,obj,date不可为null");
    }

  }

  public void delObject(String key) {
    if (key != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + key);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RBucket<Object> bucket = redissonClient.getBucket(key);
      if (bucket != null) {
        bucket.deleteAsync();
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("key不可为null");
    }

  }

  public void setMap(String mapName, String key, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (mapName != null && key != null && obj != null && timeToLive != null && timeUnit != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + mapName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RMap<Object, Object> map = redissonClient.getMap(mapName);
      map.fastPutAsync(key, obj);
      map.expire(timeToLive, timeUnit);
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("mapName,key,obj,timeToLive,timeUnit不可为null");
    }

  }

  /**
   * 添加/修改map
   * 
   * @param mapName
   *          map集合名称
   * @param key
   *          键
   * @param obj
   *          值
   */
  public void setMap(String mapName, String key, Object obj) {
    if (mapName != null && key != null && obj != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + mapName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RMap<Object, Object> map = redissonClient.getMap(mapName);
      map.fastPutAsync(key, obj);
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("mapName,key,obj不可为null");
    }

  }

  /**
   * 删除map
   * 
   * @param mapName
   *          map集合名称
   * @param key
   *          键
   */
  public void delMap(String mapName, String key) {
    if (mapName != null && key != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + mapName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RMap<Object, Object> map = redissonClient.getMap(mapName);
      if (map != null) {
        map.fastRemoveAsync(key);
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("mapName,key不可为null");
    }

  }

  public void delMap(String mapName) {
    if (mapName != null) {
      RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + mapName);
      lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
      RMap<Object, Object> map = redissonClient.getMap(mapName);
      if (map != null) {
        map.deleteAsync();
      }
      lock.writeLock().unlock();
    } else {
      throw new RuntimeException("mapName不可为null");
    }

  }

  /**
   * 删除bucket
   * 
   * @param bucketName
   * @param obj
   */
  @Deprecated
  public void delBucket(String bucketName) {
    RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + bucketName);
    lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
    RBucket<Object> bucket = redissonClient.getBucket(bucketName);
    if (bucket != null) {
      bucket.deleteAsync();
    }
    lock.writeLock().unlock();
  }

  /**
   * 添加/修改 Bucket 这个方法可以看成一个map,bucketName是key,obj是value
   * 
   * @param bucketName
   * @param obj
   */
  @Deprecated
  public void setBucket(String bucketName, Object obj) {
    setBucket(bucketName, obj, null, null);
  }

  /**
   * 
   * @see RedisService.setRBucket(String setName, Object obj)
   * @param timeToLive
   *          保存时间
   * @param timeUnit
   *          保存时间的单位
   */
  @Deprecated
  public void setBucket(String bucketName, Object obj, Long timeToLive, TimeUnit timeUnit) {
    RReadWriteLock lock = redissonClient.getReadWriteLock("Lock" + bucketName);
    lock.writeLock().lock(autoUnLockTime, TimeUnit.MILLISECONDS);
    RBucket<Object> bucket = redissonClient.getBucket(bucketName);
    if (bucket != null) {
      bucket.setAsync(obj);
      if (timeToLive != null) {
        bucket.expire(timeToLive, timeUnit);
      }
    }
    lock.writeLock().unlock();
  }
  
  public void delObjectBatch(Collection<String> collection) {
    RBatch batch = redissonClient.createBatch();
    for(String c:collection){
      batch.getBucket(c).deleteAsync();
    }
    batch.executeSkipResultAsync();
  }

  /**
   * 获得RedissonClient用来调用其他方法
   * 
   * @see https://github.com/mrniko/redisson/wiki
   * @return
   */
  public RedissonClient getRedissonClient() {
    return redissonClient;
  }
}
