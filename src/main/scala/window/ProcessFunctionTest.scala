package window

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import streamApi.SensorReading

/**
  * process Function 是唯一可以获取到实践相关的 API
  * 可以访问时间戳，waterMark
  * 可以设置定时器
  * RichFunction 能做到的 processFunction 都能做
  * 还可以输出侧输出流
  * ProcessFunction
  * KeyedProcessFunction
  * CoProcessFunction
  * ProcessJoinFunction
  * BroadProcessFunction
  * BroadCastProcessFunction
  * KeyedBroadcastProcessFunction
  * ProcessWindowFunction
  * ProcessAllWindowFunction
  *
  */
object ProcessFunctionTest {
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
    //检测每个传感器温度是否连续上升, 在10秒内
    val warningStream = dataStream.keyBy("id")
      .process(new TempIncreaseWarning(10000L))
    warningStream.print()
    env.execute()
  }

  /**
    * 自定义 keyedProcessFunction
    * @param interval
    */
  class TempIncreaseWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String]{

    //由于需要与之前的温度做对比，所以需要将上一个温度保存成状态
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    //为了方便删除定时器，需要保存定时器的时间戳
    lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curTimer", classOf[Long]))

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
      //首先去除状态
      val lastTemp = lastTempState.value()
      val curTimer = curTimerState.value()

      //将上次温度状态更新为当前温度值状态
      lastTempState.update(value.temperature)
      println("update latest temperature:" + value.temperature)

      if(value.temperature > lastTemp && curTimer == 0){
        //如果没有设置过定时器，就设置定时器
        val alarmTime = ctx.timerService().currentProcessingTime()  + interval
        println("set up alarmTime:" + alarmTime)
        println("cur time:" +  ctx.timerService().currentProcessingTime())
        ctx.timerService().registerProcessingTimeTimer(alarmTime)
        curTimerState.update(alarmTime)
      }else if(value.timestamp < lastTemp){
        //如果温度值比之前还小，就删除定时器
        ctx.timerService().deleteEventTimeTimer(curTimer)
        println("clear timmer:" + curTimer)
        curTimerState.clear()
      }
    }

    /**
      * 设置定时器的操作
      *
      * @param timestamp
      * @param ctx
      * @param out
      */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      //10S 内没有下降的温度值，就报警
      println("alarming !!!")
      out.collect("温度值连续" + interval/1000 + "秒上升")
      curTimerState.clear()
    }
  }
}
