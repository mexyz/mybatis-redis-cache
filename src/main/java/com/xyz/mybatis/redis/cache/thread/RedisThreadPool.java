package com.xyz.mybatis.redis.cache.thread;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.xyz.mybatis.redis.cache.thread.task.Task;

@Component
public class RedisThreadPool {
  private static Logger logger = Logger.getLogger(RedisThreadPool.class);
  @Value("${threadPool.corePoolSize:100}")
  private int corePoolSize;
  @Value("${threadPool.maximumPoolSize:1000}")
  private int maximumPoolSize;
  @Value("${threadPool.queueDeep:2000}")
  private int queueDeep;
  @Value("${threadPool.keepAliveTime:1}")
  private int keepAliveTime;
  @Value("${threadPool.threadSleep:1000}")
  private int threadSleep;
  private static ThreadPoolExecutor threadPoolExecutor = null;
  private static int queueDeepStatic;
  private static int threadSleepStatic;

  @PostConstruct
  public void initThreadPool() {
    queueDeepStatic = queueDeep;
    threadSleepStatic = threadSleep;
    threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(queueDeep), new ThreadPoolExecutor.DiscardOldestPolicy());

  }

  public void threadPoolExecute(Task task) {
    while (getQueueSize(threadPoolExecutor.getQueue()) >= queueDeepStatic) {
      logger.info("队列已满，等" + threadSleepStatic + "毫秒再添加任务");
      try {
        Thread.sleep(threadSleepStatic);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    threadPoolExecutor.execute(task);
  }

  public void threadPoolClose() {
    threadPoolExecutor.shutdown();
    while (true) {
      if (threadPoolExecutor.isTerminated()) {
        logger.info("任务执行完毕!");
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
  }

  private synchronized int getQueueSize(Queue<Runnable> queue) {
    return queue.size();
  }

}
