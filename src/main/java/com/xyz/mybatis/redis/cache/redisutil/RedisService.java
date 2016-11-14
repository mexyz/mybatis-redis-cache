package com.xyz.mybatis.redis.cache.redisutil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.xyz.mybatis.redis.cache.redisutil.task.RedisBucketTask;
import com.xyz.mybatis.redis.cache.redisutil.task.RedisListTask;
import com.xyz.mybatis.redis.cache.redisutil.task.RedisMapTask;
import com.xyz.mybatis.redis.cache.redisutil.task.RedisSetTask;
import com.xyz.mybatis.redis.cache.thread.RedisThreadPool;

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
  @Autowired
  private RedisThreadPool threadPool;
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
        set.add(obj);
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
        set.add(obj);
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
        set.remove(obj);
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
        set.delete();
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
        list.add(obj);
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
        list.add(obj);
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
        list.remove(obj);
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
        list.remove(index);
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
        list.delete();
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
        bucket.set(obj);
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
        bucket.set(obj);
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
        bucket.set(obj);
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
        bucket.delete();
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
      map.fastPut(key, obj);
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
      map.fastPut(key, obj);
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
        map.fastRemove(key);
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
        map.delete();
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
      bucket.delete();
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
      bucket.set(obj);
      if (timeToLive != null) {
        bucket.expire(timeToLive, timeUnit);
      }
    }
    lock.writeLock().unlock();
  }

  /**
   * 批量操作
   */

  /**
   * @see RedisService.setMap
   */
  public void setMapBatch(String mapName, String key, Object obj) {
    if (mapName != null && key != null && obj != null) {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put(key, obj);
      setMapBatch(redissonClient, mapName, map, null, null, null);
    } else {
      throw new RuntimeException("mapName,key,obj不可为null");
    }
  }

  public void setMapBatch(String mapName, Map<String, Object> map) {
    if (mapName != null && map != null) {
      setMapBatch(redissonClient, mapName, map, null, null, null);
    } else {
      throw new RuntimeException("mapName,map不可为null");
    }
  }

  /**
   * @see RedisService.setMap
   */
  public void setMapBatch(String mapName, String key, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (mapName != null && key != null && obj != null) {
      Map<String, Object> map = new HashMap<String, Object>();
      map.put(key, obj);
      setMapBatch(redissonClient, mapName, map, key, timeToLive, timeUnit);
    } else {
      throw new RuntimeException("mapName,key,obj不可为null");
    }
  }

  public void setMapBatch(String mapName, Map<String, Object> map, Long timeToLive, TimeUnit timeUnit) {
    if (mapName != null && map != null) {
      setMapBatch(redissonClient, mapName, map, null, timeToLive, timeUnit);
    } else {
      throw new RuntimeException("mapName,map不可为null");
    }
  }

  /**
   * @see RedisService.delMap
   */
  public void delMapBatch(String mapName, String key) {
    if (mapName != null && key != null) {
      setMapBatch(redissonClient, mapName, null, key, null, null);
    } else {
      throw new RuntimeException("mapName,key不可为null");
    }
  }

  public void delMapBatch(String mapName, Map<String, Object> map) {
    if (mapName != null && map != null) {
      setMapBatch(redissonClient, mapName, map, null, null, null);
    } else {
      throw new RuntimeException("mapName,map不可为null");
    }
  }

  public void delMapBatch(String mapName) {
    if (mapName != null) {
      setMapBatch(redissonClient, mapName, null, null, null, null);
    } else {
      throw new RuntimeException("mapName不可为null");
    }
  }

  /**
   * @see RedisService.setSet
   */
  public void setSetBatch(String setName, Object obj) {
    if (setName != null && obj != null) {
      Set<Object> set = new HashSet<Object>();
      set.add(obj);
      setSetBatch(setName, set, 1, null, null);
    } else {
      throw new RuntimeException("setName,obj不可为null");
    }

  }

  public void setSetBatch(String setName, Set<Object> set) {
    if (setName != null && set != null) {
      setSetBatch(setName, set, 1, null, null);
    } else {
      throw new RuntimeException("setName,set不可为null");
    }
  }

  /**
   * @see RedisService.setSet
   */
  public void setSetBatch(String setName, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (setName != null && obj != null && timeToLive != null && timeUnit != null) {
      Set<Object> set = new HashSet<Object>();
      set.add(obj);
      setSetBatch(setName, set, 1, timeToLive, timeUnit);
    } else {
      throw new RuntimeException("setName,obj,timeToLive,timeUnit不可为null");
    }

  }

  public void setSetBatch(String setName, Set<Object> set, Long timeToLive, TimeUnit timeUnit) {
    if (setName != null && set != null && timeToLive != null && timeUnit != null) {
      setSetBatch(setName, set, 1, timeToLive, timeUnit);
    } else {
      throw new RuntimeException("setName,obj,timeToLive,timeUnit不可为null");
    }
  }

  public void delSetBatch(String setName, Object obj) {
    if (setName != null && obj != null) {
      Set<Object> set = new HashSet<Object>();
      set.add(obj);
      setSetBatch(setName, set, 0, null, null);
    } else {
      throw new RuntimeException("setName,obj不可为null");
    }
  }

  public void delSetBatch(String setName, Set<Object> set) {
    if (setName != null && set != null) {
      setSetBatch(setName, set, -1, null, null);
    } else {
      throw new RuntimeException("setName,set不可为null");
    }
  }

  public void delSetBatch(String setName) {
    if (setName != null) {
      setSetBatch(setName, null, -1, null, null);
    } else {
      throw new RuntimeException("setName不可为null");
    }
  }

  /**
   * @see RedisService.setList
   */
  public void setListBatch(String listName, Object obj) {
    if (listName != null && obj != null) {
      List<Object> list = new ArrayList<Object>();
      list.add(obj);
      setListBatch(listName, list, 1, null, null);
    } else {
      throw new RuntimeException("listName,obj不可为null");
    }
  }

  public void setListBatch(String listName, List<Object> list) {
    if (listName != null && list != null) {
      setListBatch(listName, list, 1, null, null);
    } else {
      throw new RuntimeException("listName,list不可为null");
    }
  }

  public void setListBatch(String listName, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (listName != null && obj != null && timeToLive != null && timeUnit != null) {
      List<Object> list = new ArrayList<Object>();
      list.add(obj);
      setListBatch(listName, list, 1, timeToLive, timeUnit);
    } else {
      throw new RuntimeException("listName,obj,timeToLive,timeUnit不可为null");
    }
  }

  public void setListBatch(String listName, List<Object> list, Long timeToLive, TimeUnit timeUnit) {
    if (listName != null && list != null && timeToLive != null && timeUnit != null) {
      setListBatch(listName, list, 1, timeToLive, timeUnit);
    } else {
      throw new RuntimeException("listName,list,timeToLive,timeUnit不可为null");
    }
  }

  public void delListBatch(String listName, Object obj) {
    if (listName != null && obj != null) {
      List<Object> list = new ArrayList<Object>();
      list.add(obj);
      setListBatch(listName, list, 0, null, null);
    } else {
      throw new RuntimeException("listName,obj不可为null");
    }

  }

  public void delListBatch(String listName, List<Object> list) {
    if (listName != null && list != null) {
      setListBatch(listName, list, 0, null, null);
    } else {
      throw new RuntimeException("listName,list不可为null");
    }
  }

  public void delListBatch(String listName) {
    if (listName != null) {
      setListBatch(listName, null, -1, null, null);
    } else {
      throw new RuntimeException("listName不可为null");
    }
  }

  /**
   * @see RedisService.setBucket
   */
  @Deprecated
  public void setBucketBatch(String bucketName, Object obj) {
    List<Object> list = new ArrayList<Object>();
    list.add(obj);
    threadPool.threadPoolExecute(new RedisBucketTask(redissonClient, bucketName, list, 1, autoUnLockTime, null, null));
  }

  /**
   * @see RedisService.delBucket
   */
  @Deprecated
  public void delBucketBatch(String bucketName, Object obj) {
    List<Object> list = new ArrayList<Object>();
    list.add(obj);
    threadPool.threadPoolExecute(new RedisBucketTask(redissonClient, bucketName, list, 0, autoUnLockTime, null, null));
  }

  public void delObjectBatch(Collection<Object> collection) {
    threadPool.threadPoolExecute(new RedisBucketTask(redissonClient, null, collection,-1, autoUnLockTime, null, null));
  }
  /**
   * @see RedisService.setBucket
   */
  @Deprecated
  public void setBucketBatch(String bucketName, Object obj, Long timeToLive, TimeUnit timeUnit) {
    List<Object> list = new ArrayList<Object>();
    list.add(obj);
    threadPool.threadPoolExecute(
        new RedisBucketTask(redissonClient, bucketName, list, 1, autoUnLockTime, timeToLive, timeUnit));
  }

  public void setObjectBatch(String bucketName, Object obj) {
    if (bucketName != null && obj != null) {
      List<Object> list = new ArrayList<Object>();
      list.add(obj);
      setObjectBatch(bucketName, list, 1, null, null);
    } else {
      throw new RuntimeException("bucketName,obj,timeToLive,timeUnit不可为null");
    }
  }

  public void setObjectBatch(String bucketName, List<Object> list) {
    if (bucketName != null && list != null) {
      setObjectBatch(bucketName, list, 1, null, null);
    } else {
      throw new RuntimeException("bucketName,list,timeToLive,timeUnit不可为null");
    }
  }

  public void setObjectBatch(String bucketName, Object obj, Long timeToLive, TimeUnit timeUnit) {
    if (bucketName != null && obj != null && timeToLive != null && timeUnit != null) {
      List<Object> list = new ArrayList<Object>();
      list.add(obj);
      setObjectBatch(bucketName, list, 1, timeToLive, timeUnit);
    } else {
      throw new RuntimeException("bucketName,obj,timeToLive,timeUnit不可为null");
    }
  }

  private void setObjectBatch(String bucketName, List<Object> list, int type, Long timeToLive, TimeUnit timeUnit) {
    threadPool.threadPoolExecute(
        new RedisBucketTask(redissonClient, bucketName, list, type, autoUnLockTime, timeToLive, timeUnit));
  }

  private void setMapBatch(RedissonClient redissonClient, String mapName, Map<String, Object> map, String key,
      Long timeToLive, TimeUnit timeUnit) {
    threadPool
        .threadPoolExecute(new RedisMapTask(redissonClient, mapName, map, key, autoUnLockTime, timeToLive, timeUnit));
  }

  private void setSetBatch(String setName, Set<Object> setObj, int type, Long timeToLive, TimeUnit timeUnit) {
    threadPool
        .threadPoolExecute(new RedisSetTask(redissonClient, setName, null, -1, autoUnLockTime, timeToLive, timeUnit));
  }

  private void setListBatch(String listName, List<Object> obj, int type, Long timeToLive, TimeUnit timeUnit) {
    threadPool.threadPoolExecute(
        new RedisListTask(redissonClient, listName, obj, type, autoUnLockTime, timeToLive, timeUnit));
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
