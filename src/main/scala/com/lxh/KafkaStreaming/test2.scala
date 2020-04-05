package com.lxh.KafkaStreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import com.lxh.KafkaStreaming.redisWatching

/*刘晓惠
目的：
1.当程序死了来不及提交offset造成的重复消费问题
2.redis解决重复消费时造成的数据积压问题

存在的问题：这是我和一位同学交流出来的代码，但我和她都认为以这样的思路无法百分之百的解决问题，存在的问题是
当程序运行到68行（println）时宕机再次启动时，就永远无法消费到这条数据，若监视程序的35行（println）没被注释掉
就会存在重复消费的问题
 */

object test2 {

  System.setProperty("hadoop.home.dir", "F:\\Hadoop\\hadoop2.7.6_win10")

  val conf = new SparkConf().setMaster("local[*]").setAppName("test")
  val context = new SparkContext(conf)
  context.setLogLevel("WARN")
  //获取上下文对象
  val ssc = new StreamingContext(context , Seconds(5))
  //位置分布机制
  var locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent

  val brokers = "hadoop101:9092"
  val topic = "test"
  val group = "sparkaGroup"
  val kafkaParam = Map(
    "bootstrap.servers"-> brokers,
    "key.deserializer" ->classOf[StringDeserializer],
    "value.deserializer"->classOf[StringDeserializer],
    "group.id"->group,
    "auto.offset.reset"-> "latest",
    "enable.auto.commit" ->(false:java.lang.Boolean)
  );

  //消费者机制
  var consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(Array(topic), kafkaParam)

  var resultDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)

  //创建监控线程
  val watching = new redisWatching2()
  //启动监控线程
  watching.start()
  def main(args: Array[String]): Unit = {
    println("watching.start()")
    //jedis获取不能在foreachRDD里
    resultDStream.foreachRDD(iter=>{
      if(iter.count() > 0){
        iter.foreachPartition(patition =>{
          val jedis=RedisUtil("192.168.24.130",6379)
          patition.foreach(record=>{
            val value : String = record.value()
            val key="test"+record.offset().toString
            println("key="+key+" value="+value)
            if(jedis.get(record.offset().toString)==null) {
              jedis.set(key, "0")
              //消费完成
              println(value)
              //更改key的状态值,证明数据被消费过，交由监视线程处理
              jedis.set(key, "1")
              jedis.expire(key,180)
            }
          })
          jedis.close()
        })

        val ranges: Array[OffsetRange] = iter.asInstanceOf[HasOffsetRanges].offsetRanges

        resultDStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
        println("offset提交成功")
        Thread.sleep(1)
      }

    })

    ssc.start()

    ssc.awaitTermination()

  }
}

