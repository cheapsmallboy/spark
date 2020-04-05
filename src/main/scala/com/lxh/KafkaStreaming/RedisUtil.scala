package com.lxh.KafkaStreaming
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  def createJedisPool(host:String,port:Int) = {
    //创建一个连接池的配置对象
    val config = new JedisPoolConfig()
    //最大空闲连接数，默认8个
    config.setMaxIdle(1000)
    //最大连接数，默认8个
    config.setMaxTotal(2000)
    config.setMinIdle(500)
    config.setMaxWaitMillis(10000)
    //获取一个连接池
    val pool:JedisPool = new JedisPool(config,host,port)
    config.setTestOnBorrow(false)
    pool
  }
  private def getJedis(host:String,port:Int):Jedis = {

    createJedisPool(host,port).getResource
  }
  def apply(host:String,port:Int):Jedis = {
    getJedis(host,port)
  }

  def main(args: Array[String]): Unit = {
    var jedis:Jedis =RedisUtil("192.168.24.130",6379)
    jedis.set("name","xm")
    jedis.set("name1","xm1")
    jedis.set("name2","xm2")
    jedis.set("name3","xm3")
    jedis.set("name4","xm4")
    jedis.expire("name",40)
    jedis.expire("name1",40)
    jedis.expire("name2",40)
    jedis.expire("name3",40)
    jedis.expire("name4",40)

    jedis.close()
  }
}
