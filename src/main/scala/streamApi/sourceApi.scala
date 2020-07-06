package streamApi

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

import scala.util.Random

//输入数据的样例类
case class SensorReading(id:String, timestamp: Long, temperature: Double)

object streamSourceApi {
  def main(args: Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度
    env.setParallelism(1)

    //0、从 element 读取 source
    println("------从 element 中读取数据-----")
    val stream0 = env.fromElements("1", 2, "abc")
    stream0.print("stream0")

    //1、从集合中读取数据
    println("------从集合中读取数据-----")
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor1", 1593936142, 36.1),
      SensorReading("sensor2", 1593936142, 36.2),
      SensorReading("sensor3", 1593936142, 36.3),
      SensorReading("sensor4", 1593936142, 36.4)
    ))
    stream1.print("stream1")

    //2、从文件中读取数据
    println("------从文件中读取数据-----")
    val stream2 = env.readTextFile("/Users/john/Desktop/学习/tutorial/src/main/resources/sensor.tx")
    stream2.print("stream2")

    //3、socket 文本流
    //val stream3 = env.socketTextStream("localhost", 7777)

    //4、从消息队列 kafka 读取数据
    println("------从 kafka 中读取数据-----")
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")
    val stream4 = env.addSource(new FlinkKafkaConsumer09[String]("sensor", new SimpleStringSchema(), properties))
//    stream4.print()



    //5、如果 flink 没有实现连接器可以 自定义 source，也可以自动生成测试数据
    println("------ 实现自定义的 source -----")
   val stream5 = env.addSource(new MySensorFunction())
    stream5.print("stream5")

    env.execute("source test job")
  }

class MySensorFunction() extends SourceFunction[SensorReading]{

  //定义一个 flag 表示数据源是否正常运行
  var running: Boolean = true

  //随机生成数
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    //随机生成 10 个传感器的温度值，并不停更新
    var curTemps = 1.to(10).map(
      i => ("sensor" + i, 36 + rand.nextGaussian() * 20)
    )

    //无限循环生成随机数据流
    while(running){
      //获取当前温度值
      curTemps = curTemps.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      //获取当前系统时间
      val curTime = System.currentTimeMillis()
      //包装成 ctx 发出数据
      curTemps.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )
      //定义间隔时间
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = running = false
}

}
