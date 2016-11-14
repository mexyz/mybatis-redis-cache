package com.xyz.mybatis.redis.cache.redisutil.task;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RedissonClient;

import com.xyz.mybatis.redis.cache.thread.task.Task;


public class BaseTask extends Task {

  protected RedissonClient redissonClient;
  protected String name;
  protected String key = null;
  protected Object obj;
  protected int type;
  protected int autoUnLockTime;
  protected Long timeToLive = null;
  protected TimeUnit timeUnit = null;
  protected Map<String,Object> map;
  protected Collection<Object> collection;
}
