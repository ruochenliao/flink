package redisSink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import streamApi.SensorReading

object RedisSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream:DataStream[String] = env.readTextFile("/Users/john/Desktop/学习/tutorial/src/main/resources/word.tx")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    //flink 连接器使用第三方 redis sink
    val conf = new FlinkJedisPoolConfig.Builder()
        .setHost("localhost")
        .setPort(6379)
        .build()

    val redisMapper = new RedisMapper[SensorReading]{
      //保存数据到 redis 命令
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
      }

      override def getKeyFromData(t: SensorReading): String = t.id

      override def getValueFromData(t: SensorReading): String = t.temperature.toString
    }
    dataStream.addSink(new RedisSink[SensorReading](conf, redisMapper))
  }
}
