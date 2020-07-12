package TimeLatencyTest

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import streamApi.SensorReading

/**
event time      事件创建时间
ingest time     source 算子获取到数据源时间
processing time 处理到的时候的处理时间

安窗口分配
  对于乱序时间，按窗口分到不同窗口中

解决时间乱序，处理迟到数据
  多等一会，等多久，用 waterMark 去计算

waterMark 是一种衡量 Event Time 进展的机制，主要用来处理乱序事件，可以设置延迟触发，
  根据数据的最大的乱序长度来作为等待时间
    延迟时间是设置出来的数
    waterMark = maxTimestamp - 延迟时间
    当 waterMark >= 窗口关闭时间，就要关闭窗口了

  处理机制
    按照过来的流，根据时间戳分发到对应的时间窗口
    每来一个数据，计算一次 waterMark, 当 waterMark >= 窗口关闭时间，就关闭当前窗口
    每个 keyBy -> reduce 分组后，每个组都只会输出一个数据，如果有 n 个组就输出 n 个对象
  原理
    插入在数据流中的一个插入 waterMark 特殊的时间戳
    waterMark 插入的特殊数据必须单调递增
    waterMark = x, 表示 x 之前的数据都到齐了
  waterMark 在并行 task 之间传递
    上游并行的时候，按照最小的 waterMark = min(waterMark) 作为这个 task 的 waterMark
    下游, waterMark 按照广播的形式进行传递

处理丢失的乱序数据
  自己配置
  */

object TimeLatencyTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置事件语义
    val inputStream: DataStream[String] = env.socketTextStream("127.0.0.1", 7777)
    val dataStream: DataStream[SensorReading] = inputStream
      .filter(it => !it.matches(""))
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
    val resultStream = dataStream
      .keyBy("id")
      .timeWindow(Time.seconds(5)) //5s 的滚动窗口
//      .allowedLateness(Time.minutes(1))    //允许 1 minute 钟的延时, 等到 1 minutes 后，才关闭窗口，在延长的窗口内每来一条一个数据，更新输出一次结果
      .reduce((curRes, newData) =>  //对于同一组 id 下面， 输出时间最大的值，和统计输出过的最小温度
      SensorReading(curRes.id, curRes.timestamp.max(newData.timestamp), curRes.temperature.min(newData.temperature)))
        //设置 waterMark, 并设置延迟时间 = 5s, 那么 waterMark = maxTimestamp - 5s(延迟时间)； 如果 waterMark >= 窗口关闭时间，就关闭窗口
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(4)){
          //指定 element 中的 timestamp 作为 eventTime
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
        })

    resultStream.print("time test")
    env.execute()
  }
}
