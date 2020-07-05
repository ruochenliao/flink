package com.alibaba.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * 流处理 word count
  *
  * 启动方式
  * 1、启动 scala 应用
  * 2、在 terminal 使用命令启动服务 nc -lk 7777，敲单词，字符等，scala 应用会统计
  *
  * 结果结果可能是乱序，因为是多线程不同的 worker 导致的
  *
  * 并行度优先级由大到小 单个算子设置的并行度 > env 中设置的全局并行度 > 提交时的并行度 > config 中配置的并行度
  */
object StreamWordCount {
  def main(args: Array[String]): Unit ={
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;

    //可以从程序参数中传入 hostname 和 port
    val params: ParameterTool = ParameterTool.fromArgs(args)
    //动态地替代 127.0.0.1
    val hostname: String = params.get("hostname")
    //动态地替代 port
    val port: String = params.get("port")

    //接收 socket 文本流地址和端口号
    val inputDataStream : DataStream[String] = env.socketTextStream("127.0.0.1", 7777)
    //转换操作
    val resultDataStream : DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))   //空格分词打散所有的 word
      .setParallelism(2)
      .map((_, 1))                    //转换成 (word, count) 二元组
      .setParallelism(2)
      .keyBy(0)                //按照第一个元素分组， keyBy 等同于 groupBy, 根据 key 的 hash 来分组
      .sum(1)                //按照第二个元素求和

    resultDataStream.print()
    env.execute("stream word count")         //执行流式任务
  }
}
