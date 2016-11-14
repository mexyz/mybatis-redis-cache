package com.xyz.mybatis.redis.cache.conf;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.xyz.mybatis.redis.cache.annotations.MybatisCache;
import com.xyz.mybatis.redis.cache.cache.MyBatisCache;
import com.xyz.mybatis.redis.cache.redisutil.RedisService;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
/**
 * mybatis-redis缓存
 * 根据mapper中的方法和传入的参数来查询redis中的缓存
 * redis中的缓存key都经过md5,redis的key太长影响查询效率
 * MyBatisConfiguration需要继承MyBatisCacheConfiguration来开启缓
 * 默认所有select方法都会查询缓存数据
 * 在mapper方法加上@MybatisCache(disCache = true)注解可以关闭该方法的缓存,直接查数据库 
 * 
 * @author zhangyixin
 *
 */
public class MyBatisCacheConfiguration {
  /**
   * 缓存时间默认60分钟
   */
  @Value("${mybatis.cache.time:60}")
  public long cacheTime;
  /**
   * 存储数据库表和mapper中的方法对应关系,数据库表中的数据发生过更改,可以知道要清除哪个方法产生的缓存
   */
  public static Map<String, Set<String>> TABLE_METHOD = new HashMap<String, Set<String>>(100);
  /**
   * 不需要缓存的方法
   */
  public static List<String> DIS_CACHE_METHOD;

  @Bean(name="sqlSessionFactory")
  public SqlSessionFactory sqlSessionFactoryBean(DataSource ds, RedisService redisService) throws Exception {
    
    MyBatisCache myBatisCache = new MyBatisCache(redisService,cacheTime);
    Properties p = new Properties();
    p.setProperty("offsetAsPageNum", "true");
    p.setProperty("rowBoundsWithCount", "true");
    p.setProperty("reasonable", "true");
    myBatisCache.setProperties(p);
    
    SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
    sqlSessionFactoryBean.setDataSource(ds);
    sqlSessionFactoryBean.setPlugins(new Interceptor[] {myBatisCache});
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    sqlSessionFactoryBean.setMapperLocations(resolver.getResources("classpath:/mapper/*.xml"));
    Collection l = sqlSessionFactoryBean.getObject().getConfiguration().getMappedStatements();
    
    DIS_CACHE_METHOD=getDisCacheMethod(l);//mapper接口中含有@MybatisCache(disCache = true)的方法,直接查数据库
    for (Object m : l) {
      if (m instanceof MappedStatement) {
        MappedStatement ms = (MappedStatement) m;
        String sql = ms.getBoundSql(null).getSql();
        if (StringUtils.containsIgnoreCase(sql, "select")) {
          Statement statement = CCJSqlParserUtil.parse(sql);//解析select语句
          Select selectStatement = (Select) statement;
          TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
          List<String> tableList = tablesNamesFinder.getTableList(selectStatement);//取出select语句中的表名
          Set<String> tables = tables(tableList);//去重复 统一大小写 去掉特殊表字符
          methods(TABLE_METHOD, tables, ms.getId());//数据表和方法对应关系存到TABLE_METHOD
        } else if (StringUtils.containsIgnoreCase(sql, "insert")) {
          // System.out.println(sql.split("\\s+")[2]);
        } else if (StringUtils.containsIgnoreCase(sql, "delete")) {
          // System.out.println(sql.split("\\s+")[2]);
        } else if (StringUtils.containsIgnoreCase(sql, "update")) {
          // System.out.println(sql.split("\\s+")[1]);
        }

      }
    }
    return sqlSessionFactoryBean.getObject();
  }

  private Map<String, Set<String>> methods(Map<String, Set<String>> tableMethod, Set<String> tables, String method) {
    for (String table : tables) {
      if (tableMethod.get(table) == null) {
        Set<String> s = new HashSet<String>(1);
        s.add(DigestUtils.md5Hex(method));
        tableMethod.put(table, s);
      } else {
        tableMethod.get(table).add(DigestUtils.md5Hex(method));
      }
    }

    return tableMethod;
  }

  private Set<String> tables(List<String> t) {
    Set<String> s = new HashSet<String>();
    for (String tn : t) {
      s.add(tn.replaceAll("`", "").toLowerCase());
    }
    return s;
  }
  
  private List<String> getDisCacheMethod(Collection l) throws Exception{
    List<String> disCacheMethod=new ArrayList<String>();
    Set<String> allClassName=new HashSet<String>();
    for (Object m : l) {
      if (m instanceof MappedStatement) {
        MappedStatement ms = (MappedStatement) m;
        allClassName.add(StringUtils.substring(ms.getId(),0,StringUtils.lastIndexOf(ms.getId(),'.')));
      }
      
    }
    for(String className:allClassName){
      Method[] methods = Class.forName(className).getDeclaredMethods();  
      for (Method method : methods) {  
          if (method.isAnnotationPresent(MybatisCache.class)) {  
              Annotation p = method.getAnnotation(MybatisCache.class);  
              Method m = p.getClass().getDeclaredMethod("disCache", null);  
              boolean value = (boolean) m.invoke(p, null);
              if(value){
                String mt=className+"."+method.getName();
                if(!disCacheMethod.contains(mt)){
                  disCacheMethod.add(mt);
                }
              }
          }  
      }
    }
    return disCacheMethod;
  }
}
