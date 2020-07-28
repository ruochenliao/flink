package window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import streamApi.SensorReading

/**
  * waterMark 适用于处理乱序事件的，而正确处理乱序事件，通常结用 waterMark 机制结合 window 来实现
  *
  * assignTimestampsAndWatermarks
  *   周期性地插入 waterMark： AssignerWithPeriodicWatermarks，比如 BoundedOutOfOrdernessTimestampExtractor 就是周期性的 waterMark
  *   在某些条件下打点式插入 waterMark: AssignerWithPunctuatedWatermarks
  *
  */
object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //周期性地生成 waterMark
//    env.getConfig.setAutoWatermarkInterval(100L)

    //设置事件语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = env.socketTextStream("127.0.0.1", 7777)
    val resultStream = inputStream
      .filter(it => !it.matches(""))
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
//      .assignTimestampsAndWatermarks(new MyPeriodWaterMarkAssigner(1000L))
      .assignTimestampsAndWatermarks(new MyPunctualWaterMarkAssigner())
      .keyBy("id")
      .timeWindow(Time.seconds(5)) //5s 的滚动窗口
      .reduce((curRes, newData) =>{  //对于同一组 id 下面， 输出时间最大的值，和统计输出过的最小温度
      SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature))
    })
    resultStream.print("water mark test")
    env.execute("test")
  }
}

//自定义一个周期性生成 waterMark 的 assigner
class MyPeriodWaterMarkAssigner(latency: Long) extends AssignerWithPeriodicWatermarks[SensorReading]{
  //需要两个关键参数，延迟时间，和所有数据中最大的时间戳
  var maxTimeStamp: Long = Long.MinValue

  //waterMark = maxTimestamp - 延迟时间
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimeStamp - latency)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTimeStamp = Math.max(maxTimeStamp, element.timestamp * 1000L)
    element.timestamp * 1000L
  }
}

//自定义一个断电时生成 watermark 的 Assigner
class MyPunctualWaterMarkAssigner extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    val latency : Long = 1000L
    println("come")
    if(lastElement.id.equalsIgnoreCase("sensor1")){
      println("latency:"+(extractedTimestamp - latency))
      new Watermark(extractedTimestamp - latency)
    }else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    println("get timeStamp")
    element.timestamp * 1000L
  }
}
