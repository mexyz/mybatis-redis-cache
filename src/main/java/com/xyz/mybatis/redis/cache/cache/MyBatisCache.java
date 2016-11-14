package com.xyz.mybatis.redis.cache.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.apache.log4j.Logger;

import com.xyz.mybatis.redis.cache.conf.MyBatisCacheConfiguration;
import com.xyz.mybatis.redis.cache.redisutil.RedisService;

@SuppressWarnings(value = {"unused" })
@Intercepts({ @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }),
    @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class, RowBounds.class,
        ResultHandler.class }) })
public class MyBatisCache implements Interceptor {

  private static Logger log = Logger.getLogger(MyBatisCache.class);

  private RedisService redisService;
  private Properties properties;
  private long cacheTime;

  public MyBatisCache(RedisService redisService, long cacheTime) {
    this.cacheTime = cacheTime;
    this.redisService = redisService;
  }

  @Override
  public Object intercept(Invocation invocation) throws Throwable {
    try {
      Object[] args = invocation.getArgs();
      MappedStatement mappedStatement = (MappedStatement) args[0];
      Object parameter = args[1];
      BoundSql boundSql = mappedStatement.getBoundSql(parameter);
      String sql = boundSql.getSql();
      StringBuffer sb = new StringBuffer();
      String method = DigestUtils.md5Hex(mappedStatement.getId());
      if (StringUtils.equals("query", invocation.getMethod().getName())) {
        sb.append(createCacheKey(mappedStatement, parameter, (RowBounds) args[2], boundSql));
        Object parameterObject = boundSql.getParameterObject();
        if (parameterObject != null) {
          String parameterObjectType = parameterObject.getClass().getSimpleName();

          if (contains(mappedStatement.getId())) {
            log.info("读取数据库");
            return invocation.proceed();
          }

        }
        log.info(method);
        log.info(sb.toString());
        String key = DigestUtils.md5Hex(sb.toString());
        Object obj = redisService.getObject(key);
        if (obj == null) {
          obj = invocation.proceed();
          redisService.setObject(key, obj, cacheTime, TimeUnit.MINUTES);
          redisService.setSet(method, key);
          log.info("读取数据库");
          return obj;
        } else {
          log.info("读取缓存");
          return obj;

        }
      } else if (StringUtils.equals("update", invocation.getMethod().getName())) {
        if (StringUtils.containsIgnoreCase(sql, "insert")) {
          Set<String> m = MyBatisCacheConfiguration.TABLE_METHOD.get(sql.split("\\s+")[2]);
          for (String mapName : m) {
            Set<Object> set = redisService.getSet(mapName);
            redisService.delObjectBatch(set);
            redisService.delSet(mapName);
          }
        } else if (StringUtils.containsIgnoreCase(sql, "delete")) {
          Set<String> m = MyBatisCacheConfiguration.TABLE_METHOD.get(sql.split("\\s+")[2]);
          for (String mapName : m) {
            Set<Object> set = redisService.getSet(mapName);
            redisService.delObjectBatch(set);
            redisService.delSet(mapName);
          }
        } else if (StringUtils.containsIgnoreCase(sql, "update")) {
          Set<String> m = MyBatisCacheConfiguration.TABLE_METHOD.get(sql.split("\\s+")[1]);
          for (String mapName : m) {
            Set<Object> set = redisService.getSet(mapName);
            redisService.delObjectBatch(set);
            redisService.delSet(mapName);
          }
        }
        return invocation.proceed();
      } else {
        return invocation.proceed();
      }
    } catch (Exception e) {
      e.printStackTrace();
      return invocation.proceed();
    }
  }

  @Override
  public Object plugin(Object target) {
    return Plugin.wrap(target, this);
  }

  private boolean contains(String currentMethod) {
    boolean contains = false;
    for (String method : MyBatisCacheConfiguration.DIS_CACHE_METHOD) {
      if (StringUtils.equals(method, currentMethod)) {
        contains = true;
        break;
      }
    }
    return contains;
  }

 

  private Object getObjFromStr(String serStr) throws UnsupportedEncodingException, IOException, ClassNotFoundException {
    String redStr = java.net.URLDecoder.decode(serStr, "UTF-8");
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(redStr.getBytes("ISO-8859-1"));
    ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
    Object result = objectInputStream.readObject();
    objectInputStream.close();
    byteArrayInputStream.close();

    return result;
  }

  private String getStrFromObj(Object obj) throws IOException, UnsupportedEncodingException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(obj);
    String serStr = byteArrayOutputStream.toString("ISO-8859-1");
    serStr = java.net.URLEncoder.encode(serStr, "UTF-8");

    objectOutputStream.close();
    byteArrayOutputStream.close();
    return serStr;
  }

  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {

    CacheKey cacheKey = new CacheKey();
    cacheKey.update(ms.getId());
    cacheKey.update(Integer.valueOf(rowBounds.getOffset()));
    cacheKey.update(Integer.valueOf(rowBounds.getLimit()));
    cacheKey.update(boundSql.getSql());
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    TypeHandlerRegistry typeHandlerRegistry = ms.getConfiguration().getTypeHandlerRegistry();
    // mimic DefaultParameterHandler logic
    for (int i = 0; i < parameterMappings.size(); i++) {
      ParameterMapping parameterMapping = parameterMappings.get(i);
      if (parameterMapping.getMode() != ParameterMode.OUT) {
        Object value;
        String propertyName = parameterMapping.getProperty();
        if (boundSql.hasAdditionalParameter(propertyName)) {
          value = boundSql.getAdditionalParameter(propertyName);
        } else if (parameterObject == null) {
          value = null;
        } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
          value = parameterObject;
        } else {
          MetaObject metaObject = ms.getConfiguration().newMetaObject(parameterObject);
          value = metaObject.getValue(propertyName);
        }
        cacheKey.update(value);
      }
    }
    if (ms.getConfiguration().getEnvironment() != null) {
      cacheKey.update(ms.getConfiguration().getEnvironment().getId());
    }
    return cacheKey;
  }

  @Override
  public void setProperties(Properties properties) {
    // TODO Auto-generated method stub
    
  }

}
