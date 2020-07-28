package SQL

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import streamApi.SensorReading


/**
  * blink 是阿里团队开发的 SQL 支持
  */
object TableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //基于 env 创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //创建表执行环境
    val inputStream: DataStream[String] = env.readTextFile("/Users/john/Desktop/学习/tutorial/src/main/resources/sensor.tx")

    //map 成样例类类型
    val dataStream : DataStream[SensorReading] = inputStream.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })

    //基于 tableEnv, 将流转换成表
    val dataTable:Table = tableEnv.fromDataStream(dataStream)

    //调用 table api 做转换操作
    val resultTable : Table = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    //把表转换成流，打印输出
    val resultStream: DataStream[(String, Double)] = resultTable
      .toAppendStream[(String, Double)]

    resultStream.print()
    env.execute("table api")
  }
}
