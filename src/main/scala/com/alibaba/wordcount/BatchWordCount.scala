package com.alibaba.wordcount

import org.apache.flink.api.scala._

/**
  * 批处理 word count
  */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment;
    //从文件中读取数据
    val inputDataSet: DataSet[String] = env.readTextFile("/Users/john/Desktop/学习/tutorial/src/main/resources/word.tx");
    //基于 dataSet 做转换，首先按照空格分词，然后按照 word 作为 key 做 group by
    val resultDataSet: AggregateDataSet[(String, Int)] = inputDataSet.
      flatMap(_.split(" ")) //分词得到所有 word 构成的数据集
      .map((_, 1)) //转换成一个二元组 （word， count）
      .groupBy(0) //以二元组中第一个元素作为 Key 分组
      .sum(1) //聚合二元组中第二个元素的值
    println("------- 输入 --------")
    inputDataSet.print()
    println("------- 批处理完 --------")
    resultDataSet.print()
  }
}
