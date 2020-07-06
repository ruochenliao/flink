package streamApi

import org.apache.flink.streaming.api.scala._

object transformApi {
  def main(args: Array[String]): Unit = {

    println("------ map ------")
    val list: List[String] = List("1", "2", "3")
    val listMapping = list.toStream.map(
      item => item + "_suffix"
    )
    listMapping.foreach(
      item => print(item) + "; "
    )
    println()
    println("------ flapMap ------")

    //flatMap: 一条输入可以对应多条输出
    val words = Set("hello", "world", "flink")
    val character = words.flatMap(x => x.toUpperCase())
    println(character)

    println("------ filter ------")
    val filterResult = character.toStream.filter(p => p == 'h')
    println(filterResult)

    //根据 key 的 hashCode 重分区
    println("------ keyBy ------")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.readTextFile("/Users/john/Desktop/学习/tutorial/src/main/resources/sensor.tx");
    val resultStream: DataStream[SensorReading] = inputStream.map(
      data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      //      .keyBy(0)
      //      .keyBy("id")
      .keyBy(data => data.id)
      //      .sum("temperature")
      .reduce(  //聚合出每个 sensor 中，最大时间戳和最小温度值
        (curRes, newData) =>
        SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature))
    )
    resultStream.print("keyBy")

    //3、分流
    println("------ split/ select------")
    val splitStream: SplitStream[SensorReading] = resultStream.split(data =>
      if(data.temperature > 30){
        Seq("high")
      }else{
        Seq("low")
      }
    )
    val highTempStream :DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream :DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")
    highTempStream.print("high")
    lowTempStream.print("low")
    allTempStream.print("all")

    //4、合并不同类型流
    println("------ connect/ coMap------")
    val warningStream: DataStream[(String, Double)] = highTempStream.map(data =>
      (data.id, data.temperature))
    val connectedStream: ConnectedStreams[(String, Double),SensorReading] = warningStream.connect(lowTempStream)
    val connectedDataStream :DataStream[Object] = connectedStream.map(
      warningData => (warningData._1, warningData._2, "high temperature warnning"),
      lowTempData => (lowTempData.id, "normal")
    )

    connectedDataStream.print("connect stream")

    //5、合并相同类型流
    println("------ union stream ------")
    val unionStream:DataStream[SensorReading] = highTempStream.union(lowTempStream)
    unionStream.print("union stream")

    env.execute()
  }
}
