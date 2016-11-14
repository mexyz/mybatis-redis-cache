## Mybatis-redis-cache缓存
```
1.根据mapper中的方法和传入的参数来查询redis中的缓存
2.redis中的缓存key都经过md5,redis的key太长影响查询效率
3.MyBatisConfiguration需要继承MyBatisCacheConfiguration来开启缓
4.默认所有select方法都会查询缓存数据
5.在mapper方法加上@MybatisCache(disCache = true)注解可以关闭该方法的缓存,直接查数据库 
6.基于springboot
```