package com.lxh.KafkaStreaming

import java.util
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

/*
redis监视线程
存在一批数据过来后，要想在redis被标记时间，要等到下一批数据的到来
 */
class redisWatching2 extends Thread{
  override def run(): Unit =  {
    test2.resultDStream.foreachRDD(rdds=>{
      if(rdds.count() > 0){
        rdds.foreachPartition(partition => {
          //获取一个redis连接
          val jedis: Jedis = RedisUtil("192.168.24.130",6379)
          partition.foreach(record => {
            val value: String = record.value()
            //取出以"kafkaDemo."为前缀的key
            val keys = jedis.keys("test*")
            println("已经获取redis中的key")
            keys.foreach(key=>{
              println("redisWatching:"+key+" value="+jedis.get(key))
              //如果这个key没有被设置失效时间，且value值是"0",就对其进行处理
              if (jedis.ttl(key) == -1 && jedis.get(key) == "0") {
               // 防止在jedis.set(key,"0")这里宕机，没来得及设置时间
//                 println(value)
                //处理之后删除这个key
                jedis.expire(key,180)

              }
              if (jedis.ttl(key) == -1 && jedis.get(key) == "1") {
                //防止在jedis.set(key,"1")这里宕机，没来得及设置时间
                // println(value)
                //处理之后删除这个key
                jedis.expire(key,180)
              }
            })
          })
          println("over")
          jedis.close()
        })
      }
    })
  }

}
