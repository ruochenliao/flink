package state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import streamApi.SensorReading
import window.ProcessFunctionTest.TempIncreaseWarning

object state {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置 backend 状态
//    env.setStateBackend(new MemoryStateBackend())
//    env.setStateBackend(new FsStateBackend("/file"))
//    env.setStateBackend(new RocksDBStateBackend(""))

    //开启 checkpoint, 隔多久保存一次状态
//    env.enableCheckpointing(1000)

    //设置事件语义
    val inputStream: DataStream[String] = env.socketTextStream("127.0.0.1", 7777)
    val dataStream: DataStream[SensorReading] = inputStream
      .filter(it => !it.matches(""))
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    //使用 process 检测传感器差值不能过大
    val processedStream = dataStream.keyBy(_.id)
      .process(new TempChangeAlert(10.0))

    //使用 flapMap 检测传感器差值不能过大
    val processedStream2 = dataStream.keyBy(_.id)
        .flatMap(new TempChangeAlert2(10.0))

    processedStream.print("process 状态处理")
    processedStream2.print("flapMap 状态处理")

    env.execute()
  }


  /**
    * 使用
    * @param thredHold
    */
  class TempChangeAlert2(thredHold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

    private var lastTempState: ValueState[Double] = _

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      //初始化的时候申明 state 变量
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
    }

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      //获取上次的温度值
      val lastTemp = lastTempState.value()
      //用当前的温度值和上次的求差，如果大于阈值，输出报警信息
      val diff = (value.temperature - lastTemp).abs
      if(diff > thredHold){
        out.collect(value.id, lastTemp, value.temperature)
      }

      lastTempState.update(value.temperature)
    }
  }

  /**
    * 状态编程，如果温度差值大于 thredHold, 就报警
    *
    * @param thredHold
    */
  class TempChangeAlert(thredHold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)]{
    //定义一个状态变量保存上次的温度值
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
      //获取上次的温度值
      val lastTemp = lastTempState.value()
      //用当前的温度值和上次的求差，如果大于阈值，输出报警信息
      val diff = (value.temperature - lastTemp).abs
      if(diff > thredHold){
        out.collect(value.id, lastTemp, value.temperature)
      }

      lastTempState.update(value.temperature)
    }
  }
}
